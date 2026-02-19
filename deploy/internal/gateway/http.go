package gateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var validSessionIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

const (
	maxOpenBodyBytes   = 16 * 1024
	maxResumeBodyBytes = 4 * 1024
	maxResizeBodyBytes = 4 * 1024
	maxStdinBodyBytes  = 256 * 1024
	maxStdinBytes      = 64 * 1024
)

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway/sessions", h.openSession)
	mux.HandleFunc("/gateway/sessions/resume", h.resumeSession)
	mux.HandleFunc("/gateway/sessions/", h.sessionAction)
	return instrumentGatewayRequests(mux)
}

func instrumentGatewayRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		observer := &statusObserver{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(observer, r)
		log.Printf(
			"level=info event=gateway_http_request method=%s path=%q status=%d duration_ms=%d remote=%q",
			r.Method,
			r.URL.Path,
			observer.status,
			time.Since(started).Milliseconds(),
			r.RemoteAddr,
		)
	})
}

type statusObserver struct {
	http.ResponseWriter
	status int
}

func (o *statusObserver) WriteHeader(status int) {
	o.status = status
	o.ResponseWriter.WriteHeader(status)
}

func (h *Handler) openSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req struct {
		User        string            `json:"user"`
		Host        string            `json:"host"`
		Port        int               `json:"port"`
		Command     []string          `json:"command"`
		Env         map[string]string `json:"env"`
		CPUSeconds  int               `json:"cpu_seconds"`
		MemoryBytes uint64            `json:"memory_bytes"`
		MaxDuration int               `json:"max_duration_seconds"`
	}
	if err := decodeJSONBody(w, r, maxOpenBodyBytes, &req); err != nil {
		return
	}

	meta, err := h.svc.OpenSession(r.Context(), OpenSessionRequest{
		User:    req.User,
		Host:    req.Host,
		Port:    req.Port,
		Command: req.Command,
		Env:     req.Env,
		Limits: SessionLimits{
			CPUSeconds:         req.CPUSeconds,
			MemoryBytes:        req.MemoryBytes,
			MaxDurationSeconds: req.MaxDuration,
		},
	})
	if err != nil {
		writeMappedErr(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, meta)
}

func (h *Handler) resumeSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "bearer token is required")
		return
	}

	// Keep endpoint JSON shape strict even though token moved to Authorization.
	if err := decodeJSONBody(w, r, maxResumeBodyBytes, &struct{}{}); err != nil {
		return
	}

	meta, err := h.svc.ResumeSession(token)
	if err != nil {
		writeMappedErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

func (h *Handler) sessionAction(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/gateway/sessions/"), "/")
	if trimmed == "" {
		writeErr(w, http.StatusBadRequest, "BAD_PATH", "session id is required")
		return
	}

	parts := strings.Split(trimmed, "/")
	if len(parts) > 2 {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found")
		return
	}

	sid := parts[0]
	if !validSessionIDPattern.MatchString(sid) {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid session id format")
		return
	}

	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "bearer token is required")
		return
	}

	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case r.Method == http.MethodDelete && action == "":
		if err := h.svc.Close(sid, token); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && action == "stdin":
		var req struct {
			Data string `json:"data"`
		}
		if err := decodeJSONBody(w, r, maxStdinBodyBytes, &req); err != nil {
			return
		}
		payload, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_STDIN", "stdin data must be base64 encoded")
			return
		}
		if len(payload) > maxStdinBytes {
			writeErr(w, http.StatusRequestEntityTooLarge, "STDIN_TOO_LARGE", "stdin payload exceeds max size")
			return
		}
		if err := h.svc.WriteStdin(sid, token, payload); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodPost && action == "resize":
		var req struct {
			Cols int `json:"cols"`
			Rows int `json:"rows"`
		}
		if err := decodeJSONBody(w, r, maxResizeBodyBytes, &req); err != nil {
			return
		}
		if req.Cols < 1 || req.Rows < 1 || req.Cols > 4096 || req.Rows > 4096 {
			writeErr(w, http.StatusBadRequest, "BAD_RESIZE", "cols and rows must be between 1 and 4096")
			return
		}
		if err := h.svc.Resize(sid, token, uint16(req.Cols), uint16(req.Rows)); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found")
	}
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, maxBytes int64, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(target); err != nil {
		var syntaxErr *json.SyntaxError
		var maxBytesErr *http.MaxBytesError
		switch {
		case errors.As(err, &syntaxErr):
			writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
		case errors.As(err, &maxBytesErr):
			writeErr(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "request body exceeds max size")
		case strings.Contains(err.Error(), "unknown field"):
			writeErr(w, http.StatusBadRequest, "BAD_JSON", "request contains unknown fields")
		default:
			writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
		}
		return err
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must contain exactly one JSON object")
		return err
	}
	return nil
}

func bearerToken(r *http.Request) (string, bool) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if token == "" {
		return "", false
	}
	return token, true
}

func writeMappedErr(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrSessionNotFound) {
		writeErr(w, http.StatusNotFound, "SESSION_NOT_FOUND", "session could not be found or already closed")
		return
	}
	if errors.Is(err, ErrInvalidRequest) {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "request is missing required fields or uses disallowed values")
		return
	}
	if errors.Is(err, ErrUnauthorized) {
		writeErr(w, http.StatusForbidden, "FORBIDDEN", "session token does not authorize this action")
		return
	}
	if errors.Is(err, ErrSessionExpired) {
		writeErr(w, http.StatusUnauthorized, "SESSION_EXPIRED", "session token has expired")
		return
	}
	var friendly *FriendlyError
	if errors.As(err, &friendly) {
		status := http.StatusBadGateway
		if friendly.Code == "STDIN_RATE_LIMITED" {
			status = http.StatusTooManyRequests
		} else if strings.HasPrefix(friendly.Code, "SPAWN_") || friendly.Code == "PERSISTENCE_FAILED" {
			status = http.StatusServiceUnavailable
		}
		writeErr(w, status, friendly.Code, friendly.Message)
		return
	}
	writeErr(w, http.StatusInternalServerError, "INTERNAL_ERROR", "terminal gateway internal error")
}

func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message, "status": strconv.Itoa(status)})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
