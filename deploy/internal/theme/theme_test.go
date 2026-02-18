package theme

import (
	"errors"
	"testing"
)

func TestDetectTermProfileTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		term string
		want TermProfile
	}{
		{name: "xterm", term: "xterm", want: TermProfile{Colors: 16, IsTTY: true}},
		{name: "xterm-256color", term: "xterm-256color", want: TermProfile{Colors: 256, IsTTY: true}},
		{name: "screen", term: "screen", want: TermProfile{Colors: 8, IsTTY: true}},
		{name: "tmux", term: "tmux", want: TermProfile{Colors: 256, IsTTY: true}},
		{name: "linux", term: "linux", want: TermProfile{Colors: 16, IsTTY: true}},
		{name: "dumb", term: "dumb", want: TermProfile{Colors: 0, IsTTY: false}},
		{name: "empty", term: "", want: TermProfile{Colors: 0, IsTTY: false}},
		{name: "kitty truecolor", term: "xterm-kitty", want: TermProfile{Colors: 1 << 24, TrueColor: true, IsTTY: true}},
		{name: "wezterm truecolor", term: "wezterm", want: TermProfile{Colors: 1 << 24, TrueColor: true, IsTTY: true}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := detectTermProfile(tt.term)
			if got != tt.want {
				t.Fatalf("detectTermProfile(%q) = %+v, want %+v", tt.term, got, tt.want)
			}
		})
	}
}

func TestResolveImmutability(t *testing.T) {
	t.Parallel()

	first, err := Resolve(VariantWest, "wezterm")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	first.Header.Background = "#000000"

	second, err := Resolve(VariantWest, "wezterm")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if second.Header.Background != "#0B1F3A" {
		t.Fatalf("expected immutable palette, got %q", second.Header.Background)
	}
}

func TestResolveSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		variant Variant
		want    Bundle
	}{
		{variant: VariantWest, want: palettes[VariantWest]},
		{variant: VariantFitra, want: palettes[VariantFitra]},
		{variant: VariantRoot, want: palettes[VariantRoot]},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(string(tt.variant), func(t *testing.T) {
			t.Parallel()
			got, err := Resolve(tt.variant, "wezterm")
			if err != nil {
				t.Fatalf("Resolve() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("snapshot mismatch for %s:\n got=%+v\nwant=%+v", tt.variant, got, tt.want)
			}
		})
	}
}

func TestResolveUnknownVariant(t *testing.T) {
	t.Parallel()

	_, err := Resolve(Variant("mystery"), "wezterm")
	if !errors.Is(err, ErrUnknownVariant) {
		t.Fatalf("expected ErrUnknownVariant, got %v", err)
	}
}

func TestReadArchiveAlwaysGrayscale(t *testing.T) {
	t.Parallel()

	terms := []string{"xterm", "xterm-256color", "screen", "tmux", "linux", "dumb", "", "xterm-kitty", "wezterm"}
	gray := grayscaleBundle()

	for _, term := range terms {
		read, err := Resolve(VariantRead, term)
		if err != nil {
			t.Fatalf("Resolve(read, %q) unexpected error: %v", term, err)
		}
		archive, err := Resolve(VariantArchive, term)
		if err != nil {
			t.Fatalf("Resolve(archive, %q) unexpected error: %v", term, err)
		}

		if read != gray {
			t.Fatalf("read should be grayscale for %q", term)
		}
		if archive != gray {
			t.Fatalf("archive should be grayscale for %q", term)
		}
	}
}

func TestVariantCoverage(t *testing.T) {
	t.Parallel()

	if len(palettes) != len(variants) {
		t.Fatalf("variant coverage mismatch: palettes=%d variants=%d", len(palettes), len(variants))
	}

	for _, v := range variants {
		if _, ok := palettes[v]; !ok {
			t.Fatalf("missing palette for variant %q", v)
		}
	}
}

func TestResolveForceOverrides(t *testing.T) {
	t.Parallel()

	color, _, err := resolveWithProfile(VariantWest, ResolveOptions{Term: "xterm-256color", ForceColor: true}, detectTermProfile)
	if err != nil {
		t.Fatalf("resolveWithProfile error: %v", err)
	}
	if color == grayscaleBundle() {
		t.Fatalf("force color should not return grayscale bundle")
	}

	mono, _, err := resolveWithProfile(VariantWest, ResolveOptions{Term: "wezterm", ForceMono: true}, detectTermProfile)
	if err != nil {
		t.Fatalf("resolveWithProfile error: %v", err)
	}
	if mono != grayscaleBundle() {
		t.Fatalf("force mono should return grayscale bundle")
	}
}
