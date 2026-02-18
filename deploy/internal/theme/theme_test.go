package theme

import "testing"

func TestResolveUsesMonochromeForXTerm256Color(t *testing.T) {
	bundle := Resolve(VariantWest, "xterm-256color")
	if bundle != grayscaleBundle() {
		t.Fatalf("expected grayscale bundle for xterm-256color")
	}
}

func TestResolveUsesVariantPaletteForHigherCapabilityTerm(t *testing.T) {
	bundle := Resolve(VariantFitra, "wezterm")
	if bundle.Header.Background != "#D4AF37" {
		t.Fatalf("expected fitra palette header background, got %q", bundle.Header.Background)
	}
}

func TestResolveReadAndArchiveAreGrayscale(t *testing.T) {
	read := Resolve(VariantRead, "wezterm")
	archive := Resolve(VariantArchive, "wezterm")
	gray := grayscaleBundle()

	if read != gray {
		t.Fatalf("expected read palette to be grayscale")
	}
	if archive != gray {
		t.Fatalf("expected archive palette to be grayscale")
	}
}
