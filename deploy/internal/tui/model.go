package tui

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	statusLiveOn  = "STATUS: [LIVE]"
	statusLiveOff = "STATUS: [    ]"
	promptPrefix  = "MSC-USER ~ $ "
)

// Screen identifies the active top-level TUI view.
type Screen int

const (
	ScreenMOTD Screen = iota
	ScreenTriage
	ScreenCommand
)

// Message types consumed by Update.
type (
	TickMsg       struct{}
	CursorTickMsg struct{}
	KeyMsg        struct{ Key string }
	ResizeMsg     struct{ Width, Height int }
)

// Model represents the terminal UI state with three panes.
type Model struct {
	width  int
	height int

	viewportLines []string
	viewportTop   int
	viewportH     int

	statusBlink bool
	cursorBlink bool

	screen Screen

	observerHash   string
	promptInput    string
	selectedVector string
}

// NewModel constructs the interactive TUI model from caller/session metadata.
func NewModel(remoteAddr string, width, height int) Model {
	m := Model{
		width:         width,
		height:        height,
		viewportH:     max(height-6, 1),
		statusBlink:   true,
		cursorBlink:   true,
		screen:        ScreenMOTD,
		observerHash:  deriveObserverHash(remoteAddr),
		viewportLines: strings.Split(renderMOTD(), "\n"),
	}
	return m
}

// NextStatusTick returns a helper duration for callers that schedule blink updates.
func NextStatusTick() time.Duration { return 450 * time.Millisecond }

// NextCursorTick returns a helper duration for callers that schedule prompt cursor blinking.
func NextCursorTick() time.Duration { return 530 * time.Millisecond }

// Update advances model state in response to events.
func (m Model) Update(msg any) Model {
	switch msg := msg.(type) {
	case ResizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewportH = max(m.height-6, 1)
	case TickMsg:
		m.statusBlink = !m.statusBlink
	case CursorTickMsg:
		m.cursorBlink = !m.cursorBlink
	case KeyMsg:
		switch m.screen {
		case ScreenMOTD:
			if msg.Key == "enter" {
				m.screen = ScreenTriage
				m.setViewportContent(renderTriageMenu())
			}
		case ScreenTriage:
			switch strings.ToLower(msg.Key) {
			case "a":
				m.activateVector("VECTOR_A", "READ")
			case "b":
				m.activateVector("VECTOR_B", "ARCHIVE")
			case "c":
				m.activateVector("VECTOR_C", "RETURN")
			}
		case ScreenCommand:
			switch msg.Key {
			case "enter":
				line := strings.TrimSpace(m.promptInput)
				m.promptInput = ""
				if line != "" {
					m.viewportLines = append(m.viewportLines, promptPrefix+line)
					m.viewportTop = max(len(m.viewportLines)-m.viewportH, 0)
				}
			case "backspace":
				if len(m.promptInput) > 0 {
					m.promptInput = m.promptInput[:len(m.promptInput)-1]
				}
			default:
				if len(msg.Key) == 1 {
					m.promptInput += msg.Key
				}
			}
		}
	}

	return m
}

func (m *Model) activateVector(vector, mode string) {
	m.selectedVector = vector
	m.screen = ScreenCommand
	m.setViewportContent(fmt.Sprintf("TRIAGE SELECTION: %s => %s\n\nAwaiting command input.", mode, vector))
}

// View renders three-pane display with fixed header, dynamic viewport, and prompt.
func (m Model) View() string {
	return strings.Join([]string{
		m.renderHeader(),
		m.renderViewport(),
		m.renderPrompt(),
	}, "\n")
}

func (m Model) renderHeader() string {
	status := statusLiveOff
	if m.statusBlink {
		status = statusLiveOn
	}

	return strings.Join([]string{
		"MOSAIC PROTOCOL v.1.0 // NODE: GENESIS_BLOCK",
		status,
		fmt.Sprintf("OBSERVER: [%s]", m.observerHash),
	}, "\n")
}

func (m Model) renderViewport() string {
	if len(m.viewportLines) == 0 {
		return ""
	}

	from := min(max(m.viewportTop, 0), len(m.viewportLines)-1)
	to := min(from+m.viewportH, len(m.viewportLines))
	if from >= to {
		return ""
	}
	return strings.Join(m.viewportLines[from:to], "\n")
}

func (m Model) renderPrompt() string {
	if m.screen != ScreenCommand {
		return promptPrefix + "[PRESS ENTER TO CONTINUE]"
	}

	cursor := " "
	if m.cursorBlink {
		cursor = "█"
	}

	return promptPrefix + m.promptInput + cursor
}

func (m *Model) setViewportContent(content string) {
	m.viewportLines = strings.Split(content, "\n")
	m.viewportTop = 0
}

func deriveObserverHash(remoteAddr string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(remoteAddr)))
	return strings.ToUpper(hex.EncodeToString(sum[:]))[:12]
}

func renderMOTD() string {
	return strings.Join([]string{
		"WELCOME TO MOSAIC TERMINAL",
		"--------------------------------",
		"Message of the Day:",
		"- Integrity channel synchronized.",
		"- Genesis telemetry online.",
		"- Triage vectors available after acknowledgement.",
		"",
		"Press ENTER to open triage menu.",
	}, "\n")
}

func renderTriageMenu() string {
	return strings.Join([]string{
		"TRIAGE MENU // SELECT A VECTOR",
		"A) READ    -> VECTOR_A",
		"B) ARCHIVE -> VECTOR_B",
		"C) RETURN  -> VECTOR_C",
		"",
		"Press A, B, or C.",
	}, "\n")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
