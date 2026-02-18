package tui

import (
	"fmt"
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

func TestObserverHashDerivationCases(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "ipv4", in: "198.51.100.14:2048", want: "19D336FB0E33"},
		{name: "ipv6", in: "[2001:db8::1]:2222", want: "28E931B5B859"},
		{name: "empty", in: "", want: "E3B0C44298FC"},
		{name: "malformed", in: "not-an-addr", want: "B2A8A057290D"},
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
		{name: "motd-direct-hotkey", keys: []string{"a"}, wantScreen: ScreenCommand, wantVector: "VECTOR_A"},
		{name: "motd-triage-command", keys: []string{"enter", "b"}, wantScreen: ScreenCommand, wantVector: "VECTOR_B"},
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

func TestInvalidKeysAreNoOp(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	before := m
	m = m.Update(KeyMsg{Key: "z"}) // invalid in MOTD
	if m.screen != before.screen || m.selectedVector != before.selectedVector {
		t.Fatalf("motd invalid key should be no-op")
	}

	m = m.Update(KeyMsg{Key: "enter"}) // to triage
	before = m
	m = m.Update(KeyMsg{Key: "?"})
	if m.screen != before.screen || m.selectedVector != before.selectedVector {
		t.Fatalf("triage invalid key should be no-op")
	}

	m = m.Update(KeyMsg{Key: "a"}) // to command
	beforePrompt := m.promptInput
	m = m.Update(KeyMsg{Key: "enter"}) // empty enter
	if m.promptInput != beforePrompt {
		t.Fatalf("empty enter should preserve empty prompt")
	}
}

func TestNoReenterMOTDAfterCommandStarts(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "a"})
	if m.screen != ScreenCommand {
		t.Fatalf("expected command")
	}
	m = m.Update(KeyMsg{Key: "esc"})
	if m.screen != ScreenCommand {
		t.Fatalf("command esc should be no-op and never reenter MOTD")
	}
}

func TestExtremeResizeNeverPanicsOrGoesNegative(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	sizes := []ResizeMsg{{Width: 0, Height: 0}, {Width: -10, Height: -1}, {Width: 1, Height: 1}, {Width: 9999, Height: 2}}
	for _, sz := range sizes {
		m = m.Update(sz)
		if m.width < 1 || m.height < 1 || m.viewportH < 1 {
			t.Fatalf("invalid dimensions after resize: %+v", m)
		}
		_ = m.View()
	}
}

func TestSimultaneousTicksNoStateDrift(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	for i := 0; i < 100; i++ {
		m = m.Update(TickMsg{})
		m = m.Update(CursorTickMsg{})
		m = m.Update(TickMsg{})
		m = m.Update(CursorTickMsg{})
	}
	if !m.statusBlink || !m.cursorBlink {
		t.Fatalf("even number of toggles should restore initial blink states")
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
	if !strings.Contains(m.View(), "[PRESS ENTER TO CONTINUE / A/B/C TO SELECT VECTOR]") {
		t.Fatalf("expected non-tty prompt hint")
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
	if !strings.Contains(motdView, "Press Enter to continue / A/B/C to select vector.") {
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
		if m.viewportH < 1 {
			t.Fatalf("invalid viewport height")
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
