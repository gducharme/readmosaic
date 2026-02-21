package tui

import (
	"fmt"
	"mosaic-terminal/internal/theme"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-runewidth"
)

type fixedTicks struct{}

func (fixedTicks) StatusTick() time.Duration     { return 10 * time.Millisecond }
func (fixedTicks) CursorTick() time.Duration     { return 20 * time.Millisecond }
func (fixedTicks) TypewriterTick() time.Duration { return 30 * time.Millisecond }

type customTicks struct {
	status     time.Duration
	cursor     time.Duration
	typewriter time.Duration
}

func (t customTicks) StatusTick() time.Duration     { return t.status }
func (t customTicks) CursorTick() time.Duration     { return t.cursor }
func (t customTicks) TypewriterTick() time.Duration { return t.typewriter }

func pumpTypewriter(m Model) Model {
	for i := 0; i < 4096; i++ {
		before := strings.Join(m.viewportLines, "\n")
		m = m.Update(TypewriterTickMsg{})
		after := strings.Join(m.viewportLines, "\n")
		if after == before && !m.typewriterActive && len(m.typewriterQueue) == 0 {
			break
		}
	}
	return m
}

func TestNewModelStartsOnMOTD(t *testing.T) {
	m := NewModelWithOptions("192.0.2.7:2222", Options{Width: 80, Height: 24, IsTTY: true, Ticks: fixedTicks{}})
	if m.screen != ScreenMOTD {
		t.Fatalf("expected MOTD screen, got %v", m.screen)
	}
	if !strings.Contains(m.View(), "Message of the Day") {
		t.Fatalf("expected MOTD content in viewport")
	}
	if m.NextStatusTick() != 10*time.Millisecond || m.NextCursorTick() != 20*time.Millisecond || m.NextTypewriterTick() != 30*time.Millisecond {
		t.Fatalf("unexpected tick durations")
	}
}

func TestTypewriterCadenceIsConfigurable(t *testing.T) {
	ticks := customTicks{
		status:     111 * time.Millisecond,
		cursor:     222 * time.Millisecond,
		typewriter: 7 * time.Millisecond,
	}
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Ticks: ticks})
	if got := m.NextTypewriterTick(); got != 7*time.Millisecond {
		t.Fatalf("unexpected typewriter cadence: %v", got)
	}
	if got := m.NextStatusTick(); got != 111*time.Millisecond {
		t.Fatalf("unexpected status cadence: %v", got)
	}
	if got := m.NextCursorTick(); got != 222*time.Millisecond {
		t.Fatalf("unexpected cursor cadence: %v", got)
	}
}

func TestTypewriterCadenceFromEnv(t *testing.T) {
	t.Setenv(typewriterTickMsEnvVar, "9")
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true})
	if got := m.NextTypewriterTick(); got != 9*time.Millisecond {
		t.Fatalf("unexpected typewriter cadence from env: %v", got)
	}
}

func TestTypewriterBatchStepConfigurable(t *testing.T) {
	t.Setenv(typewriterBatchEnvVar, "3")
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true})
	m.setViewportContent("seed")
	m.enqueueTypewriter("abcdef")
	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "abc" {
		t.Fatalf("expected 3-grapheme step, got %q", got)
	}

	m2 := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, TypewriterStep: 2})
	m2.setViewportContent("seed")
	m2.enqueueTypewriter("abcdef")
	m2 = m2.Update(TypewriterTickMsg{})
	if got := m2.viewportLines[len(m2.viewportLines)-1]; got != "ab" {
		t.Fatalf("expected option override step=2, got %q", got)
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
	if m.viewportH < 1 {
		t.Fatalf("viewportH must be >= 1")
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
	if m.viewportH != 1 {
		t.Fatalf("expected viewportH=1 for height=7, got %d", m.viewportH)
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
	m = pumpTypewriter(m)

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
	m = pumpTypewriter(m)

	if !strings.Contains(renderViewport(m), readFallbackLine) {
		t.Fatalf("expected fallback line when fragment file is unavailable")
	}
}

func TestVectorSelectionUsesTypewriterAcrossOptions(t *testing.T) {
	cases := []struct {
		name   string
		key    string
		vector string
	}{
		{name: "read-a", key: "a", vector: "VECTOR_A"},
		{name: "archive-b", key: "b", vector: "VECTOR_B"},
		{name: "return-c", key: "c", vector: "VECTOR_C"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewModel("127.0.0.1:1234", 80, 24)
			m = m.Update(KeyMsg{Key: "enter"})
			m = m.Update(KeyMsg{Key: tc.key})
			if strings.Contains(renderViewport(m), "Awaiting command input.") {
				t.Fatalf("line should not appear before queue drains")
			}
			m = pumpTypewriter(m)
			if !strings.Contains(renderViewport(m), "CONFIRMED VECTOR: "+tc.vector) {
				t.Fatalf("missing confirmed vector line after typewriter")
			}
			if !strings.Contains(renderViewport(m), "Awaiting command input.") {
				t.Fatalf("missing awaited prompt line after typewriter")
			}
		})
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

func TestThemeBundleIgnoredWhenNonTTY(t *testing.T) {
	bundle := theme.Bundle{StyleSet: theme.StyleSet{
		Header:   theme.Style{Foreground: "#FFFFFF", Background: "#000000", Bold: true},
		Viewport: theme.Style{Foreground: "#112233", Background: "#445566"},
		Prompt:   theme.Style{Foreground: "#778899", Background: "#AABBCC", Bold: true},
	}}
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: false, ThemeBundle: &bundle})
	if strings.Contains(m.View(), "[") {
		t.Fatalf("non-tty output must not contain ANSI escapes")
	}
}

func TestNilThemeBundleMatchesDefaultRendering(t *testing.T) {
	m1 := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true})
	m2 := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, ThemeBundle: nil})
	if got, want := m2.View(), m1.View(); got != want {
		t.Fatalf("nil ThemeBundle should match default rendering")
	}
}

func TestApplyStyleInvalidAndEmptyStylesNoOp(t *testing.T) {
	content := "alpha\nbeta"
	cases := []theme.Style{
		{},
		{Foreground: "#FFF"},
		{Background: "#12345"},
		{Foreground: "#GGGGGG"},
		{Background: "not-a-color"},
	}
	for _, style := range cases {
		if got := applyStyle(content, style); got != content {
			t.Fatalf("expected no-op for style=%+v", style)
		}
	}
}

func TestApplyStyleMultilineResetsEachLine(t *testing.T) {
	content := "first\nsecond"
	styled := applyStyle(content, theme.Style{Foreground: "#010203", Bold: true})
	lines := strings.Split(styled, "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, "\x1b[38;2;1;2;3;1m") {
			t.Fatalf("line %d missing style prefix: %q", i, line)
		}
		if !strings.HasSuffix(line, "\x1b[0m") {
			t.Fatalf("line %d missing reset suffix: %q", i, line)
		}
	}
}

func TestThemeBundleIsCopiedAtConstruction(t *testing.T) {
	bundle := theme.Bundle{StyleSet: theme.StyleSet{
		Header: theme.Style{Foreground: "#FFFFFF", Background: "#000000", Bold: true},
	}}
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, ThemeBundle: &bundle})
	bundle.Header.Foreground = "#FF00FF"
	view := m.View()
	if strings.Contains(view, "255;0;255") {
		t.Fatalf("model should not reflect post-construction theme mutations")
	}
	if !strings.Contains(view, "255;255;255") {
		t.Fatalf("expected original copied style to be used")
	}
}

func TestThemeBundleAppliesANSIStyles(t *testing.T) {
	bundle := theme.Bundle{StyleSet: theme.StyleSet{
		Header:   theme.Style{Foreground: "#FFFFFF", Background: "#000000", Bold: true},
		Viewport: theme.Style{Foreground: "#112233", Background: "#445566"},
		Prompt:   theme.Style{Foreground: "#778899", Background: "#AABBCC", Bold: true},
	}}
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, ThemeBundle: &bundle})
	view := m.View()
	if !strings.Contains(view, "\x1b[38;2;255;255;255;48;2;0;0;0;1mMOSAIC PROTOCOL") {
		t.Fatalf("expected ANSI-styled header")
	}
	if !strings.Contains(view, "\x1b[38;2;119;136;153;48;2;170;187;204;1mMSC-USER") {
		t.Fatalf("expected ANSI-styled prompt")
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
	m = pumpTypewriter(m)
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

func TestTypewriterTickNoOpWhenQueueEmpty(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	before := strings.Join(m.viewportLines, "\n")
	m = m.Update(TypewriterTickMsg{})
	after := strings.Join(m.viewportLines, "\n")
	if before != after {
		t.Fatalf("typewriter tick should not mutate viewport when queue is empty")
	}
	if m.typewriterActive || len(m.typewriterQueue) != 0 {
		t.Fatalf("typewriter state should remain idle")
	}
}

func TestTypewriterSingleLineQueueTransitionsCleanly(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("DONE")
	if !m.typewriterActive {
		t.Fatalf("expected active typewriter for single line")
	}
	for i := 0; i < 10; i++ {
		m = m.Update(TypewriterTickMsg{})
	}
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "DONE" {
		t.Fatalf("unexpected rendered line: %q", got)
	}
	if m.typewriterActive || len(m.typewriterQueue) != 0 {
		t.Fatalf("single line queue should be fully drained")
	}
}

func TestTypewriterUsesGraphemeBoundariesAndStableWidths(t *testing.T) {
	line := "👩🏽\u200d💻e\u0301界"
	expected := []string{"👩🏽\u200d💻", "👩🏽\u200d💻e\u0301", "👩🏽\u200d💻e\u0301界"}
	expectedWidths := []int{2, 3, 5}

	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter(line)

	for i := range expected {
		m = m.Update(TypewriterTickMsg{})
		got := m.viewportLines[len(m.viewportLines)-1]
		if got != expected[i] {
			t.Fatalf("tick %d rendered %q, want %q", i, got, expected[i])
		}
		if width := runewidth.StringWidth(got); width != expectedWidths[i] {
			t.Fatalf("tick %d width=%d want %d", i, width, expectedWidths[i])
		}
	}

}

func TestTypewriterUsesLogicalPrefixRevealForRTL(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("مرحبا")

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "م" {
		t.Fatalf("first RTL tick rendered %q, want %q", got, "م")
	}

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "مر" {
		t.Fatalf("second RTL tick rendered %q, want %q", got, "مر")
	}
}

func TestTypewriterStillUsesPrefixRevealForLTR(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("hello")

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "h" {
		t.Fatalf("first LTR tick rendered %q, want %q", got, "h")
	}

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "he" {
		t.Fatalf("second LTR tick rendered %q, want %q", got, "he")
	}
}

func TestTypewriterMixedDirectionUsesFirstStrongCharacter(t *testing.T) {
	line := "Error: الملف not found"
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter(line)

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "E" {
		t.Fatalf("first mixed-direction tick rendered %q, want %q", got, "E")
	}

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "Er" {
		t.Fatalf("second mixed-direction tick rendered %q, want %q", got, "Er")
	}

	m = pumpTypewriter(m)
	if got := m.viewportLines[len(m.viewportLines)-1]; got != line {
		t.Fatalf("mixed-direction line finished as %q, want %q", got, line)
	}
}

func TestTypewriterRTLWithNumbersUsesLogicalPrefixReveal(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("١٢٣مرحبا")

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "١" {
		t.Fatalf("first RTL+number tick rendered %q, want %q", got, "١")
	}
}

func TestTypewriterNeutralOnlyLineDefaultsToLTR(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("١٢٣")

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "١" {
		t.Fatalf("neutral-only line tick rendered %q, want %q", got, "١")
	}
}

func TestTypewriterWhitespaceLineDoesNotLeakRTLState(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("مرحبا", "   ", "hello")
	m = pumpTypewriter(m)

	if got := m.viewportLines[len(m.viewportLines)-1]; got != "hello" {
		t.Fatalf("unexpected final line after whitespace transition: %q", got)
	}
	if m.typewriterRTL {
		t.Fatalf("rtl state leaked after queue drained")
	}
}

func TestTypewriterRTLCombiningMarksRemainStable(t *testing.T) {
	line := "سّ" // sheen + shadda
	if clusters := toGraphemeClusters(line); len(clusters) != 1 {
		t.Fatalf("expected single grapheme cluster for %q, got %d", line, len(clusters))
	}

	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter(line)

	m = m.Update(TypewriterTickMsg{})
	if got := m.viewportLines[len(m.viewportLines)-1]; got != line {
		t.Fatalf("rtl combining grapheme rendered %q, want %q", got, line)
	}
}

func TestTypewriterTracksDirectionAcrossMultilineSwitches(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("مرحبا", "hello", "مرحبا")

	if !m.typewriterRTL {
		t.Fatalf("expected first queued line direction to be RTL")
	}

	for i := 0; i < 5; i++ {
		m = m.Update(TypewriterTickMsg{})
	}
	m = m.Update(TypewriterTickMsg{}) // begin second line
	if m.typewriterRTL {
		t.Fatalf("expected second queued line direction to be LTR")
	}
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "h" {
		t.Fatalf("expected second line first tick to be %q, got %q", "h", got)
	}

	for i := 0; i < 4; i++ {
		m = m.Update(TypewriterTickMsg{})
	}
	m = m.Update(TypewriterTickMsg{}) // begin third line
	if !m.typewriterRTL {
		t.Fatalf("expected third queued line direction to be RTL")
	}
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "م" {
		t.Fatalf("expected third line first tick to be %q, got %q", "م", got)
	}
}

func TestTypewriterFlushDuringRTLAnimation(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("مرحبا", "later")
	m = m.Update(TypewriterTickMsg{})
	m.flushTypewriter()

	if got := m.viewportLines[len(m.viewportLines)-2]; got != "مرحبا" {
		t.Fatalf("expected flushed active RTL line, got %q", got)
	}
	if got := m.viewportLines[len(m.viewportLines)-1]; got != "later" {
		t.Fatalf("expected flushed queued line, got %q", got)
	}
	if m.typewriterActive || len(m.typewriterQueue) != 0 || m.typewriterRTL {
		t.Fatalf("expected typewriter state reset after flush")
	}
}

func TestTypewriterQueueOverflowWhileAnimatingRTL(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("مرحبا")

	extra := make([]string, 0, maxTypewriterQueueLines+10)
	for i := 0; i < maxTypewriterQueueLines+10; i++ {
		extra = append(extra, fmt.Sprintf("tail-%03d", i))
	}
	m.enqueueTypewriter(extra...)

	if !m.typewriterRTL {
		t.Fatalf("expected active RTL direction to remain set while queue overflows")
	}
	if len(m.typewriterQueue) > maxTypewriterQueueLines {
		t.Fatalf("queue should remain capped, got %d", len(m.typewriterQueue))
	}

	for i := 0; i < 5; i++ {
		m = m.Update(TypewriterTickMsg{})
	}
	m = m.Update(TypewriterTickMsg{}) // begin next queued line (drop marker)
	if strings.Join(m.typewriterTarget, "") != typewriterQueueDropLine {
		t.Fatalf("expected drop marker to become next animated line")
	}
	if m.typewriterRTL {
		t.Fatalf("expected drop marker line direction to resolve LTR")
	}
}

func TestTypewriterQueueLimitDropsOldest(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	lines := make([]string, 0, maxTypewriterQueueLines+20)
	for i := 0; i < maxTypewriterQueueLines+20; i++ {
		lines = append(lines, fmt.Sprintf("L-%03d", i))
	}
	m.enqueueTypewriter(lines...)
	if len(m.typewriterQueue) > maxTypewriterQueueLines {
		t.Fatalf("queue should be capped: %d", len(m.typewriterQueue))
	}
	if m.typewriterActive {
		if strings.Join(m.typewriterTarget, "") != typewriterQueueDropLine {
			t.Fatalf("expected drop marker as first active line")
		}
	} else if len(m.typewriterQueue) > 0 && m.typewriterQueue[0] != typewriterQueueDropLine {
		t.Fatalf("expected drop marker at queue head")
	}
}

func TestAppendLineIsEnqueuedWhenTypewriterActive(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	m.setViewportContent("seed")
	m.enqueueTypewriter("hello")
	beforeLines := len(m.viewportLines)
	m.appendViewportLine("runtime")
	if len(m.viewportLines) != beforeLines {
		t.Fatalf("append should not write directly while typewriter is active")
	}
	if len(m.typewriterQueue) == 0 || m.typewriterQueue[len(m.typewriterQueue)-1] != "runtime" {
		t.Fatalf("runtime line should be queued behind active animation")
	}
}

func TestTypewriterFlushesWhenUserInteractsMidAnimation(t *testing.T) {
	tmp := t.TempDir()
	fragmentPath := filepath.Join(tmp, "vector_a.txt")
	if err := os.WriteFile(fragmentPath, []byte("line-1\nline-2"), 0o600); err != nil {
		t.Fatalf("write fragment: %v", err)
	}
	t.Setenv(readFragmentPathEnvVar, fragmentPath)

	m := NewModel("127.0.0.1:1234", 80, 24)
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "a"})

	if strings.Contains(renderViewport(m), "Awaiting command input.") {
		t.Fatalf("expected animation to be in progress before user input")
	}

	m = m.Update(KeyMsg{Key: "x"})
	viewport := renderViewport(m)
	if !strings.Contains(viewport, "Awaiting command input.") || !strings.Contains(viewport, "line-2") {
		t.Fatalf("expected flush to complete queued lines, got: %q", viewport)
	}
	if m.promptInput != "x" {
		t.Fatalf("expected user key to remain in prompt input, got %q", m.promptInput)
	}
	if m.typewriterActive || len(m.typewriterQueue) != 0 {
		t.Fatalf("typewriter should be idle after flush")
	}
}

func TestNewModelStartsWithNoTypewriterState(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)
	if m.typewriterActive || len(m.typewriterQueue) != 0 || m.typewriterCursor != 0 {
		t.Fatalf("new model should start with empty typewriter state")
	}
}

func TestTwoModelsAdvanceTypewriterIndependently(t *testing.T) {
	m1 := NewModel("127.0.0.1:1234", 80, 24)
	m2 := NewModel("127.0.0.2:1234", 80, 24)

	m1 = m1.Update(KeyMsg{Key: "enter"})
	m2 = m2.Update(KeyMsg{Key: "enter"})
	m1 = m1.Update(KeyMsg{Key: "a"})
	m2 = m2.Update(KeyMsg{Key: "b"})

	before2 := strings.Join(m2.viewportLines, "\n")
	m1 = m1.Update(TypewriterTickMsg{})
	after2 := strings.Join(m2.viewportLines, "\n")
	if before2 != after2 {
		t.Fatalf("model2 should not change when only model1 is ticked")
	}

	m2 = m2.Update(TypewriterTickMsg{})
	if strings.Join(m2.viewportLines, "\n") == before2 {
		t.Fatalf("model2 should change when its own typewriter tick is applied")
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

func TestArchiveUserStartsInLanguageMenu(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "ar"), 0o755); err != nil {
		t.Fatalf("mkdir ar: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	if m.screen != ScreenArchiveLanguage {
		t.Fatalf("expected archive language screen, got %v", m.screen)
	}
	view := renderViewport(m)
	if !strings.Contains(view, "English (en)") || !strings.Contains(view, "العربية (ar)") {
		t.Fatalf("expected autonym + code labels, got: %q", view)
	}
	expectedLine := "1) العربية (ar) -> " + filepath.Join(root, "ar")
	if !strings.Contains(view, expectedLine) {
		t.Fatalf("expected explicit menu line %q, got: %q", expectedLine, view)
	}
}

func TestArchiveFlowStartsInLanguageMenuForReadUser(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read", Flow: "archive"})
	if m.screen != ScreenArchiveLanguage {
		t.Fatalf("expected archive language screen for archive flow, got %v", m.screen)
	}
	if !strings.Contains(renderPrompt(m), "[ARCHIVE LANGUAGE #]") {
		t.Fatalf("expected archive language prompt, got %q", renderPrompt(m))
	}
}

func TestArchiveFlowNormalizationStartsForReadUser(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read", Flow: " ARCHIVE "})
	if m.screen != ScreenArchiveLanguage {
		t.Fatalf("expected normalized archive flow to start archive mode, got %v", m.screen)
	}
}

func TestUnknownFlowDoesNotTriggerArchiveForNonArchiveUser(t *testing.T) {
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read", Flow: "read"})
	if m.screen != ScreenMOTD {
		t.Fatalf("expected non-archive flow to preserve default screen, got %v", m.screen)
	}
}

func TestArchiveModePrecedenceBetweenUsernameAndFlow(t *testing.T) {
	t.Run("username archive flow empty", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
			t.Fatalf("mkdir en: %v", err)
		}
		t.Setenv(archiveRootEnvVar, root)
		t.Setenv(archiveSeedEnvVar, "false")

		m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive", Flow: ""})
		if m.screen != ScreenArchiveLanguage {
			t.Fatalf("expected archive username fallback to archive screen, got %v", m.screen)
		}
	})

	t.Run("username read flow archive", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
			t.Fatalf("mkdir en: %v", err)
		}
		t.Setenv(archiveRootEnvVar, root)
		t.Setenv(archiveSeedEnvVar, "false")

		m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read", Flow: "archive"})
		if m.screen != ScreenArchiveLanguage {
			t.Fatalf("expected explicit archive flow to start archive screen, got %v", m.screen)
		}
	})
}

func TestDefaultBehaviorUnchangedWithoutFlowForNonArchiveUser(t *testing.T) {
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read"})
	if m.screen != ScreenMOTD {
		t.Fatalf("expected default MOTD for non-archive user without flow, got %v", m.screen)
	}
}

func TestTriageMenuDeduplicatesImmediateRepeatedSelectionKey(t *testing.T) {
	now := time.Unix(1700000000, 0)
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read"})
	m.now = func() time.Time { return now }
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenTriage {
		t.Fatalf("expected triage screen, got %v", m.screen)
	}
	m = m.Update(KeyMsg{Key: "b"})
	if m.screen != ScreenCommand {
		t.Fatalf("expected command screen after first selection, got %v", m.screen)
	}
	m = m.Update(KeyMsg{Key: "b"})
	if m.promptInput != "" {
		t.Fatalf("expected duplicate triage selection key not to leak into command input, got %q", m.promptInput)
	}
}

func TestArchiveMenuDeduplicatesImmediateRepeatedDigitInput(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	if err := os.WriteFile(filepath.Join(langDir, "001-Intro"), []byte("Hello"), 0o600); err != nil {
		t.Fatalf("write sample file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	now := time.Unix(1700000000, 0)
	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m.now = func() time.Time { return now }

	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "1"})
	if m.promptInput != "1" {
		t.Fatalf("expected immediate duplicate digit to be ignored, got %q", m.promptInput)
	}
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenArchiveFile {
		t.Fatalf("expected to enter archive file menu, got %v", m.screen)
	}

	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "1"})
	if m.promptInput != "1" {
		t.Fatalf("expected duplicate file digit to be ignored, got %q", m.promptInput)
	}
	now = now.Add(menuSelectionDebounce + time.Millisecond)
	m = m.Update(KeyMsg{Key: "1"})
	if m.promptInput != "11" {
		t.Fatalf("expected delayed repeated digit to be accepted, got %q", m.promptInput)
	}
}

func TestRootFamilyUsersStartInArchiveLanguageMenu(t *testing.T) {
	for _, user := range []string{"root", "fitra", "west"} {
		t.Run(user, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "en"), 0o755); err != nil {
				t.Fatalf("mkdir en: %v", err)
			}
			t.Setenv(archiveRootEnvVar, root)
			t.Setenv(archiveSeedEnvVar, "false")

			m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: user, Flow: "vector"})
			if m.screen != ScreenArchiveLanguage {
				t.Fatalf("expected archive language screen for %s, got %v", user, m.screen)
			}
		})
	}
}

func TestArchiveEditorReadOnlyForRootFamilyUsers(t *testing.T) {
	for _, user := range []string{"root", "fitra", "west"} {
		t.Run(user, func(t *testing.T) {
			root := t.TempDir()
			langDir := filepath.Join(root, "en")
			if err := os.Mkdir(langDir, 0o755); err != nil {
				t.Fatalf("mkdir en: %v", err)
			}
			filePath := filepath.Join(langDir, "001-Intro")
			if err := os.WriteFile(filePath, []byte("Hello"), 0o600); err != nil {
				t.Fatalf("write initial file: %v", err)
			}
			t.Setenv(archiveRootEnvVar, root)
			t.Setenv(archiveSeedEnvVar, "false")

			m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: user, Flow: "vector"})
			m = m.Update(KeyMsg{Key: "1"})
			m = m.Update(KeyMsg{Key: "enter"})
			m = m.Update(KeyMsg{Key: "1"})
			m = m.Update(KeyMsg{Key: "enter"})
			if m.screen != ScreenArchiveEditor {
				t.Fatalf("expected archive editor screen, got %v", m.screen)
			}
			if got := renderPrompt(m); !strings.Contains(got, "[READ ONLY") {
				t.Fatalf("expected read-only prompt, got %q", got)
			}
			if !m.typewriterActive && len(m.typewriterQueue) == 0 {
				t.Fatalf("expected typewriter playback to be active or queued")
			}
			for i := 0; i < 8; i++ {
				m = m.Update(TypewriterTickMsg{})
			}
			if got := renderViewport(m); !strings.Contains(got, "Hello") {
				t.Fatalf("expected typewriter playback to render file content, got %q", got)
			}

			m = m.Update(KeyMsg{Key: "!"})
			if got := renderViewport(m); !strings.Contains(got, "Read-only mode: edits are disabled") {
				t.Fatalf("expected read-only feedback message, got %q", got)
			}
			updated, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("read updated file: %v", err)
			}
			if string(updated) != "Hello" {
				t.Fatalf("expected read-only file to stay unchanged, got %q", string(updated))
			}
		})
	}
}

func TestArchiveEditorReadOnlyEscAndCtrlD(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	if err := os.WriteFile(filepath.Join(langDir, "001-Intro"), []byte("Hello"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "root", Flow: "vector"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenArchiveEditor {
		t.Fatalf("expected archive editor screen, got %v", m.screen)
	}

	m = m.Update(KeyMsg{Key: "esc"})
	if m.screen != ScreenArchiveFile {
		t.Fatalf("expected esc to return to archive file screen, got %v", m.screen)
	}

	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "ctrl+d"})
	if m.screen != ScreenExit {
		t.Fatalf("expected ctrl+d to exit, got %v", m.screen)
	}
}

func TestArchiveFlowReadUserRemainsEditable(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("Hello"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "read", Flow: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenArchiveEditor {
		t.Fatalf("expected archive editor screen, got %v", m.screen)
	}
	if got := renderPrompt(m); !strings.Contains(got, "[EDITING LIVE") {
		t.Fatalf("expected editable prompt, got %q", got)
	}

	m = m.Update(KeyMsg{Key: "!"})
	updated, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file: %v", err)
	}
	if string(updated) != "Hello!" {
		t.Fatalf("expected editable archive flow for read user, got %q", string(updated))
	}
}

func TestArchiveEditorPersistsEditsImmediately(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("Hello"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenArchiveFile {
		t.Fatalf("expected archive file screen, got %v", m.screen)
	}
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenArchiveEditor {
		t.Fatalf("expected archive editor screen, got %v", m.screen)
	}

	m = m.Update(KeyMsg{Key: "!"})
	updated, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file: %v", err)
	}
	if string(updated) != "Hello!" {
		t.Fatalf("expected immediate persistence, got %q", string(updated))
	}

	m = m.Update(KeyMsg{Key: "left"})
	m = m.Update(KeyMsg{Key: "X"})
	updated, err = os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file after cursor insert: %v", err)
	}
	if string(updated) != "HelloX!" {
		t.Fatalf("expected cursor-aware insertion, got %q", string(updated))
	}

	m = m.Update(KeyMsg{Key: "backspace"})
	updated, err = os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file after backspace: %v", err)
	}
	if string(updated) != "Hello!" {
		t.Fatalf("expected persisted backspace edit, got %q", string(updated))
	}
}

func TestArchiveEditorVerticalNavigationAndPromptBlinkMarker(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("A\nBBBB"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})

	m = m.Update(KeyMsg{Key: "home"})
	m = m.Update(KeyMsg{Key: "down"})
	m = m.Update(KeyMsg{Key: "right"})
	m = m.Update(KeyMsg{Key: "Z"})

	updated, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file after navigation insert: %v", err)
	}
	if string(updated) != "A\nBZBBB" {
		t.Fatalf("expected vertical navigation insertion, got %q", string(updated))
	}

	prompt := renderPrompt(m)
	if !strings.Contains(prompt, "[EDITING LIVE") || (!strings.Contains(prompt, "◉") && !strings.Contains(prompt, "◌")) {
		t.Fatalf("expected blinking editing marker in prompt, got %q", prompt)
	}
}

func TestArchiveEditorCursorNewlineBoundarySemantics(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("AB\nCD"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m.archiveCursor = 2                // AB|\nCD
	m = m.Update(KeyMsg{Key: "right"}) // next line start
	line, col := archiveCursorLineCol(m.archiveEditorBuffer, m.archiveCursor)
	if line != 2 || col != 1 {
		t.Fatalf("expected cursor at line 2 col 1 after crossing newline, got line=%d col=%d", line, col)
	}
	m = m.Update(KeyMsg{Key: "right"})
	line, col = archiveCursorLineCol(m.archiveEditorBuffer, m.archiveCursor)
	if line != 2 || col != 2 {
		t.Fatalf("expected cursor at line 2 col 2 after moving right, got line=%d col=%d", line, col)
	}

	m = m.Update(KeyMsg{Key: "up"})
	line, col = archiveCursorLineCol(m.archiveEditorBuffer, m.archiveCursor)
	if line != 1 || col != 2 {
		t.Fatalf("expected up to preserve visual column on previous line, got line=%d col=%d", line, col)
	}
}

func TestArchiveEditorVerticalNavigationShorterLineClamp(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("LONG\nS"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "home"})
	for i := 0; i < 4; i++ {
		m = m.Update(KeyMsg{Key: "right"})
	}
	m = m.Update(KeyMsg{Key: "down"})
	line, col := archiveCursorLineCol(m.archiveEditorBuffer, m.archiveCursor)
	if line != 2 || col != 2 { // after single rune "S", cursor clamps to end-of-line
		t.Fatalf("expected clamp to line2 col2, got line=%d col=%d", line, col)
	}
}

func TestRenderArchiveBufferWithCursorInvariants(t *testing.T) {
	buf := "AB\nCD"
	lines := renderArchiveBufferWithCursor(buf, 2, true)
	joined := strings.Join(lines, "\n")
	if strings.Count(joined, "█") != 1 {
		t.Fatalf("expected exactly one cursor glyph, got %q", joined)
	}
	if len(lines) != len(strings.Split(buf, "\n")) {
		t.Fatalf("expected same rendered line count, got=%d want=%d", len(lines), len(strings.Split(buf, "\n")))
	}
}

func TestArchiveEditorMarksRTLDirection(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "ar")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir ar: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("مرحبا"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})

	if !strings.Contains(renderViewport(m), "[RTL]") {
		t.Fatalf("expected RTL marker in editor header")
	}
}

func TestArchivePersistenceRejectsPathOutsideRoot(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	inside := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(inside, []byte("Hello"), 0o600); err != nil {
		t.Fatalf("write inside file: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "evil.txt")
	if err := os.WriteFile(outside, []byte("bad"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m.archiveEditPath = outside
	m.archiveEditorBuffer = "changed"
	m.persistArchiveEdit()
	if got := m.archiveStatus; !strings.Contains(got, "outside archive root") {
		t.Fatalf("expected containment warning, got %q", got)
	}
	content, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("read outside file: %v", err)
	}
	if string(content) != "bad" {
		t.Fatalf("outside file must stay unchanged, got %q", string(content))
	}
}

func TestArchiveEditorFiltersControlRunesFromInputChunk(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("Hi"), 0o600); err != nil {
		t.Fatalf("write initial file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})

	m = m.Update(KeyMsg{Key: "A" + string(rune(0)) + "B"})
	updated, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read updated file: %v", err)
	}
	if string(updated) != "HiAB" {
		t.Fatalf("expected control rune filtered but printable runes kept, got %q", string(updated))
	}
}

func TestArchiveOpenRejectsLargeFiles(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.Mkdir(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	large := strings.Repeat("x", maxArchiveFileBytes+1)
	if err := os.WriteFile(filePath, []byte(large), 0o600); err != nil {
		t.Fatalf("write large file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})

	if !strings.Contains(m.archiveStatus, "file too large") {
		t.Fatalf("expected file too large warning, got %q", m.archiveStatus)
	}
	if m.archiveEditorBuffer != "" {
		t.Fatalf("large file should not be loaded into editor buffer")
	}
}

func TestArchiveSeedContentCreatesDefaultLanguageDirectoriesAndFiles(t *testing.T) {
	root := t.TempDir()
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "true")

	_ = NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})

	checks := []struct {
		dir  string
		file string
	}{
		{dir: "en", file: "001-Intro"},
		{dir: "fr", file: "001-Intro"},
		{dir: "ar", file: "001-Intro"},
	}
	for _, check := range checks {
		path := filepath.Join(root, check.dir, check.file)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected seed file %s to exist: %v", path, err)
		}
	}
}

func TestArchiveContainmentRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outsideDir := t.TempDir()
	outsideFile := filepath.Join(outsideDir, "escape.txt")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	langDir := filepath.Join(root, "en")
	if err := os.MkdirAll(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	linkPath := filepath.Join(langDir, "001-Link")
	if err := os.Symlink(outsideFile, linkPath); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	if m.isPathInsideArchiveRoot(linkPath) {
		t.Fatalf("symlink escaping archive root should be rejected")
	}
}

func TestArchivePersistRejectsOversizedBuffer(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.MkdirAll(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	if err := os.WriteFile(filePath, []byte("seed"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m.archiveEditPath = filePath
	m.archiveEditorBuffer = strings.Repeat("x", maxArchiveFileBytes+1)
	m.persistArchiveEdit()
	if !strings.Contains(m.archiveStatus, "exceeds max size") {
		t.Fatalf("expected oversize error, got %q", m.archiveStatus)
	}
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(content) != "seed" {
		t.Fatalf("oversize write should not mutate file, got %q", string(content))
	}
}

func TestArchiveOpenSanitizesControlRunesFromFileContent(t *testing.T) {
	root := t.TempDir()
	langDir := filepath.Join(root, "en")
	if err := os.MkdirAll(langDir, 0o755); err != nil {
		t.Fatalf("mkdir en: %v", err)
	}
	filePath := filepath.Join(langDir, "001-Intro")
	content := "ok" + string(rune(0x1b)) + "bad\nline\tkeep"
	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	t.Setenv(archiveRootEnvVar, root)
	t.Setenv(archiveSeedEnvVar, "false")

	m := NewModelWithOptions("127.0.0.1:1234", Options{Width: 80, Height: 24, IsTTY: true, Username: "archive"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})
	m = m.Update(KeyMsg{Key: "1"})
	m = m.Update(KeyMsg{Key: "enter"})

	if strings.ContainsRune(m.archiveEditorBuffer, rune(0x1b)) {
		t.Fatalf("expected escape rune to be sanitized from loaded content")
	}
	if !strings.Contains(m.archiveEditorBuffer, "line\tkeep") {
		t.Fatalf("expected newline/tab-preserved content, got %q", m.archiveEditorBuffer)
	}
}
