package tui

import (
	"strings"
	"testing"
)

func TestNewModelStartsOnMOTD(t *testing.T) {
	m := NewModel("192.0.2.7:2222", 80, 24)

	if m.screen != ScreenMOTD {
		t.Fatalf("expected MOTD screen, got %v", m.screen)
	}

	if !strings.Contains(m.View(), "Message of the Day") {
		t.Fatalf("expected MOTD content in viewport")
	}
}

func TestObserverHashDerivationStable(t *testing.T) {
	got := deriveObserverHash("198.51.100.14:2048")
	want := "19D336FB0E33"
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestTriageSelectionMovesToCommandPrompt(t *testing.T) {
	m := NewModel("127.0.0.1:1234", 80, 24)

	m = m.Update(KeyMsg{Key: "enter"})
	if m.screen != ScreenTriage {
		t.Fatalf("expected triage screen, got %v", m.screen)
	}

	m = m.Update(KeyMsg{Key: "b"})
	if m.screen != ScreenCommand {
		t.Fatalf("expected command screen, got %v", m.screen)
	}
	if m.selectedVector != "VECTOR_B" {
		t.Fatalf("expected VECTOR_B, got %s", m.selectedVector)
	}
	if !strings.Contains(m.View(), "MSC-USER ~ $") {
		t.Fatalf("expected command prompt in view")
	}
}
