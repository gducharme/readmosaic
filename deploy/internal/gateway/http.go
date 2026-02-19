package gateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	return mux
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
		return
	}
	meta, err := h.svc.OpenSession(r.Context(), OpenSessionRequest{
		User: req.User, Host: req.Host, Port: req.Port, Command: req.Command, Env: req.Env,
		Limits: SessionLimits{CPUSeconds: req.CPUSeconds, MemoryBytes: req.MemoryBytes, MaxDuration: time.Duration(req.MaxDuration) * time.Second},
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
	var req struct {
		ResumeToken string `json:"resume_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
		return
	}
	meta, err := h.svc.ResumeSession(req.ResumeToken)
	if err != nil {
		writeMappedErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

func (h *Handler) sessionAction(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/gateway/sessions/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusBadRequest, "BAD_PATH", "session id is required")
		return
	}
	sid := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch {
	case r.Method == http.MethodDelete && action == "":
		if err := h.svc.Close(sid); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && action == "stdin":
		var req struct {
			Data string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
			return
		}
		payload, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_STDIN", "stdin data must be base64 encoded")
			return
		}
		if err := h.svc.WriteStdin(sid, payload); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodPost && action == "resize":
		var req struct {
			Cols int `json:"cols"`
			Rows int `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_JSON", "request body must be valid JSON")
			return
		}
		if req.Cols < 1 || req.Rows < 1 || req.Cols > 4096 || req.Rows > 4096 {
			writeErr(w, http.StatusBadRequest, "BAD_RESIZE", "cols and rows must be between 1 and 4096")
			return
		}
		if err := h.svc.Resize(sid, uint16(req.Cols), uint16(req.Rows)); err != nil {
			writeMappedErr(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found")
	}
}

func writeMappedErr(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrSessionNotFound) {
		writeErr(w, http.StatusNotFound, "SESSION_NOT_FOUND", "session could not be found or already closed")
		return
	}
	if errors.Is(err, ErrInvalidRequest) {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "request is missing required fields")
		return
	}
	var friendly *FriendlyError
	if errors.As(err, &friendly) {
		status := http.StatusBadGateway
		if strings.HasPrefix(friendly.Code, "SPAWN_") {
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
