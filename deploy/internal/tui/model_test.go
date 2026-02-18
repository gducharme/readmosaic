package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fixedTicks struct{}

func (fixedTicks) StatusTick() time.Duration { return 10 * time.Millisecond }
func (fixedTicks) CursorTick() time.Duration { return 20 * time.Millisecond }

func TestNewModelStartsOnMOTD(t *testing.T) {
	m := NewModelWithOptions("192.0.2.7:2222", Options{Width: 80, Height: 24, IsTTY: true, Ticks: fixedTicks{}})
	if m.screen != ScreenMOTD {
		t.Fatalf("expected MOTD screen, got %v", m.screen)
	}
	if !strings.Contains(m.View(), "Message of the Day") {
		t.Fatalf("expected MOTD content in viewport")
	}
	if m.NextStatusTick() != 10*time.Millisecond || m.NextCursorTick() != 20*time.Millisecond {
		t.Fatalf("unexpected tick durations")
	}
}

func TestRemoteAddrNormalizationCases(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "127.0.0.1:1234", want: "127.0.0.1"},
		{in: "127.0.0.1", want: "127.0.0.1"},
		{in: " [::1]:1234 ", want: "::1"},
		{in: "::1", want: "::1"},
	}
	for _, tc := range cases {
		if got := normalizeRemoteAddr(tc.in); got != tc.want {
			t.Fatalf("normalizeRemoteAddr(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestObserverHashDerivationCases(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "ipv4", in: "198.51.100.14:2048", want: deriveObserverHash("198.51.100.14:2048")},
		{name: "ipv6_equivalent_1", in: "[::1]:1234", want: deriveObserverHash("::1")},
		{name: "empty", in: "", want: "E3B0C44298FC"},
		{name: "malformed", in: "not-an-addr", want: deriveObserverHash("not-an-addr")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveObserverHash(tc.in); got != tc.want {
				t.Fatalf("expected %s, got %s", tc.want, got)
			}
		})
	}
}

func TestStateMachineTransitionsTable(t *testing.T) {
	cases := []struct {
		name       string
		keys       []string
		wantScreen Screen
		wantVector string
	}{
		{name: "motd-enter-triage", keys: []string{"enter"}, wantScreen: ScreenTriage},
		{name: "motd-triage-command", keys: []string{"enter", "b"}, wantScreen: ScreenCommand, wantVector: "VECTOR_B"},
		{name: "motd-triage-command-uppercase", keys: []string{"enter", "A"}, wantScreen: ScreenCommand, wantVector: "VECTOR_A"},
		{name: "command-exit", keys: []string{"enter", "c", "ctrl+d"}, wantScreen: ScreenExit, wantVector: "VECTOR_C"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewModel("127.0.0.1:1234", 80, 24)
			for _, key := range tc.keys {
				m = m.Update(KeyMsg{Key: key})
			}
			if m.screen != tc.wantScreen {
				t.Fatalf("expected screen %v, got %v", tc.wantScreen, m.screen)
			}
			if tc.wantVector != "" && m.selectedVector != tc.wantVector {
				t.Fatalf("expected vector %s, got %s", tc.wantVector, m.selectedVector)
			}
		})
	}
}

func TestInvalidKeysAreNoOpInEachMode(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	before := m
	m = m.Update(KeyMsg{Key: "z"})
	if m.screen != before.screen || m.selectedVector != before.selectedVector {
		t.Fatalf("motd invalid key should be no-op")
	}

	m = m.Update(KeyMsg{Key: "enter"})
	before = m
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != before.screen {
		t.Fatalf("triage enter should be no-op")
	}
	m = m.Update(KeyMsg{Key: "?"})
	if m.screen != before.screen {
		t.Fatalf("triage invalid key should be no-op")
	}

	m = m.Update(KeyMsg{Key: "a"})
	beforeVector := m.selectedVector
	m = m.Update(KeyMsg{Key: "b"})
	if m.selectedVector != beforeVector {
		t.Fatalf("command b should append prompt text, not change vector")
	}
}

func TestNoReenterMOTDAfterCommandStarts(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})
	if m.screen != ScreenCommand {
		t.Fatalf("expected command")
	}
	m = m.Update(KeyMsg{Key: "esc"})
	if m.screen != ScreenCommand {
		t.Fatalf("command esc should be no-op and never reenter MOTD")
	}
}

func TestResizeClampViewportBounds(t *testing.T) {
	m := NewModelWithOptions("", Options{Width: 80, Height: 24, IsTTY: true, MaxBufferLines: 64})
	for i := 0; i < 20; i++ {
		m = m.Update(AppendLineMsg{Line: fmt.Sprintf("line-%d", i)})
	}
	m.viewportTop = 999

	m = m.Update(ResizeMsg{Width: 0, Height: 0})
	if m.viewportH < 0 {
		t.Fatalf("viewportH must be >= 0")
	}
	maxTop := max(len(m.viewportLines)-m.viewportH, 0)
	if m.viewportTop < 0 || m.viewportTop > maxTop {
		t.Fatalf("viewportTop out of bounds: top=%d max=%d", m.viewportTop, maxTop)
	}
	_ = m.View()
}

func TestViewportHeightReservesHeaderAndPromptChrome(t *testing.T) {
	m := NewModelWithOptions("", Options{Width: 80, Height: 24, IsTTY: true})
	if m.viewportH != 17 {
		t.Fatalf("expected viewportH=17 for height=24, got %d", m.viewportH)
	}
	m = m.Update(ResizeMsg{Width: 80, Height: 7})
	if m.viewportH != 0 {
		t.Fatalf("expected viewportH=0 for height=7, got %d", m.viewportH)
	}
}

func TestTickTogglesDeterministic(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	status0, cursor0 := m.statusBlink, m.cursorBlink
	m = m.Update(TickMsg{})
	if m.statusBlink == status0 {
		t.Fatalf("status should toggle once per TickMsg")
	}
	m = m.Update(CursorTickMsg{})
	if m.cursorBlink == cursor0 {
		t.Fatalf("cursor should toggle once per CursorTickMsg")
	}
}

func TestNonTTYDegradesGracefully(t *testing.T) {
	m := NewModelWithOptions("", Options{Width: 80, Height: 24, IsTTY: false})
	before := m
	m = m.Update(TickMsg{})
	m = m.Update(CursorTickMsg{})
	if m.statusBlink != before.statusBlink || m.cursorBlink != before.cursorBlink {
		t.Fatalf("non-tty should not blink-toggle")
	}
	if !strings.Contains(m.View(), "[PRESS ENTER TO CONTINUE]") {
		t.Fatalf("expected non-tty prompt hint")
	}
}

func TestTriagePromptHintIsModeSpecific(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	if !strings.Contains(m.View(), "[PRESS A/B/C TO SELECT, ESC TO RETURN]") {
		t.Fatalf("expected triage-specific prompt hint")
	}
}

func TestPromptEnterSubmitsAndClearsInput(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})
	m = m.Update(KeyMsg{Key: "h"})
	m = m.Update(KeyMsg{Key: "i"})
	m = m.Update(KeyMsg{Key: "enter"})
	if m.promptInput != "" {
		t.Fatalf("enter should clear prompt input")
	}
	if !strings.Contains(renderViewport(m), "MSC-USER ~ $ hi") {
		t.Fatalf("enter should append submitted command to viewport")
	}
}

func TestVectorAReadLoadsFragmentFromRuntimeFile(t *testing.T) {
	tmp := t.TempDir()
	fragmentPath := filepath.Join(tmp, "vector_a.txt")
	if err := os.WriteFile(fragmentPath, []byte("Lorem ipsum runtime line\nSecond line"), 0o600); err != nil {
		t.Fatalf("write fragment: %v", err)
	}

	t.Setenv(readFragmentPathEnvVar, fragmentPath)

	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})

	viewport := renderViewport(m)
	if !strings.Contains(viewport, "READ PAYLOAD:") {
		t.Fatalf("expected read payload heading")
	}
	if !strings.Contains(viewport, "Lorem ipsum runtime line") || !strings.Contains(viewport, "Second line") {
		t.Fatalf("expected runtime fragment content, got: %q", viewport)
	}
}

func TestVectorAReadFallsBackWhenFragmentMissing(t *testing.T) {
	t.Setenv(readFragmentPathEnvVar, filepath.Join(t.TempDir(), "missing.txt"))

	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})

	if !strings.Contains(renderViewport(m), readFallbackLine) {
		t.Fatalf("expected fallback line when fragment file is unavailable")
	}
}

func TestManyTicksDoNotGrowViewportOrChangeMode(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	mode := m.screen
	lines := len(m.viewportLines)
	for i := 0; i < 5000; i++ {
		m = m.Update(TickMsg{})
		m = m.Update(CursorTickMsg{})
	}
	if m.screen != mode {
		t.Fatalf("ticks must not alter screen mode")
	}
	if len(m.viewportLines) != lines {
		t.Fatalf("ticks must not append viewport lines")
	}
}

func TestPromptBackspaceEditing(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})
	m = m.Update(KeyMsg{Key: "世"})
	m = m.Update(KeyMsg{Key: "界"})
	m = m.Update(KeyMsg{Key: "backspace"})
	if m.promptInput != "世" {
		t.Fatalf("expected backspace to remove one rune without UTF-8 corruption, got %q", m.promptInput)
	}
}

func TestViewportBufferCapAndAppendContract(t *testing.T) {
	m := NewModelWithOptions("", Options{Width: 80, Height: 10, IsTTY: true, MaxBufferLines: 5})
	for i := 0; i < 20; i++ {
		m = m.Update(AppendLineMsg{Line: fmt.Sprintf("line-%d", i)})
	}
	if len(m.viewportLines) != 5 {
		t.Fatalf("expected capped buffer=5, got %d", len(m.viewportLines))
	}
	if m.viewportLines[0] != "line-15" {
		t.Fatalf("expected trimmed leading line")
	}
}

func TestGoldenRenders(t *testing.T) {
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true})
	motdView := m.View()
	if !strings.Contains(motdView, "MOSAIC PROTOCOL v.1.0 // NODE: GENESIS_BLOCK\nSTATUS: [LIVE]") {
		t.Fatalf("motd header snapshot mismatch")
	}
	if !strings.Contains(motdView, "Press Enter to continue.") {
		t.Fatalf("motd golden mismatch")
	}

	m = m.Update(KeyMsg{Key: "enter"})
	triageView := m.View()
	if !strings.Contains(triageView, "TRIAGE MENU // SELECT A VECTOR") {
		t.Fatalf("triage golden mismatch")
	}

	m = m.Update(KeyMsg{Key: "b"})
	cmdView := m.View()
	if !strings.Contains(cmdView, "VECTOR: [VECTOR_B]") || !strings.Contains(cmdView, "CONFIRMED VECTOR: VECTOR_B") {
		t.Fatalf("command golden mismatch")
	}
}

func TestRuntimeEventWiringAndReplaceContract(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(ReplaceViewportMsg{Content: "x\ny"})
	if got := renderViewport(m); got != "x\ny" {
		t.Fatalf("replace contract failed: %q", got)
	}
	m = m.Update(StatusUpdateMsg{Status: "healthy"})
	if !strings.Contains(renderViewport(m), "STATUS UPDATE: healthy") {
		t.Fatalf("status update missing")
	}
}

func FuzzViewportScrollingLogic(f *testing.F) {
	f.Add(5, 10)
	f.Add(1, 0)
	f.Add(300, 2)
	f.Fuzz(func(t *testing.T, appendCount int, resizeHeight int) {
		if appendCount < 0 {
			appendCount = -appendCount
		}
		appendCount = appendCount % 1000
		m := NewModelWithOptions("", Options{Width: 80, Height: 24, IsTTY: true, MaxBufferLines: 64})
		for i := 0; i < appendCount; i++ {
			m = m.Update(AppendLineMsg{Line: fmt.Sprintf("f-%d", i)})
		}
		m = m.Update(ResizeMsg{Width: 1, Height: resizeHeight})
		m = m.Update(KeyMsg{Key: "enter"})
		m = m.Update(KeyMsg{Key: "a"})
		_ = m.View()
		if len(m.viewportLines) > m.maxBuffer {
			t.Fatalf("buffer overflow")
		}
		if m.viewportH < 0 {
			t.Fatalf("invalid viewport height")
		}
		maxTop := max(len(m.viewportLines)-m.viewportH, 0)
		if m.viewportTop < 0 || m.viewportTop > maxTop {
			t.Fatalf("viewport top out of bounds")
		}
	})
}

func BenchmarkRenderPerTick(b *testing.B) {
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 100, Height: 40, IsTTY: true})
	for i := 0; i < 300; i++ {
		m = m.Update(AppendLineMsg{Line: fmt.Sprintf("line-%d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m = m.Update(TickMsg{})
		_ = Render(m)
	}
}
