package theme

import "strings"

// Variant identifies the thematic palette/style family.
type Variant string

const (
	VariantWest    Variant = "west"
	VariantFitra   Variant = "fitra"
	VariantRoot    Variant = "root"
	VariantRead    Variant = "read"
	VariantArchive Variant = "archive"
)

// Style describes presentational attributes for a UI element.
type Style struct {
	Foreground string
	Background string
	Bold       bool
}

// Bundle contains all display styles needed by the UI surface.
type Bundle struct {
	Header      Style
	Viewport    Style
	Prompt      Style
	Warning     Style
	DossierCard Style
}

// Resolve returns the style bundle for a variant and TERM profile.
//
// If TERM implies an xterm-256color (or lower) capability profile,
// the returned styles are monochrome/high-contrast to preserve readability.
func Resolve(variant Variant, term string) Bundle {
	if useMonochrome(term) {
		return highContrastBundle(variant)
	}

	switch variant {
	case VariantWest:
		return westBundle()
	case VariantFitra:
		return fitraBundle()
	case VariantRoot:
		return rootBundle()
	case VariantRead, VariantArchive:
		return grayscaleBundle()
	default:
		return grayscaleBundle()
	}
}

func useMonochrome(term string) bool {
	n := strings.ToLower(strings.TrimSpace(term))
	if n == "" {
		return true
	}

	switch n {
	case "xterm-256color", "xterm", "vt100", "screen", "dumb", "ansi":
		return true
	}

	if strings.Contains(n, "256") || strings.Contains(n, "color") {
		return true
	}

	return false
}

func westBundle() Bundle {
	return Bundle{
		Header:      Style{Foreground: "#FFFFFF", Background: "#0B1F3A", Bold: true},
		Viewport:    Style{Foreground: "#D7E3F4", Background: "#122A4A"},
		Prompt:      Style{Foreground: "#FFFFFF", Background: "#0F345E", Bold: true},
		Warning:     Style{Foreground: "#FFDDE0", Background: "#5B1F2A", Bold: true},
		DossierCard: Style{Foreground: "#EAF1FB", Background: "#163A63"},
	}
}

func fitraBundle() Bundle {
	return Bundle{
		Header:      Style{Foreground: "#1A1A1A", Background: "#D4AF37", Bold: true},
		Viewport:    Style{Foreground: "#D2FFE8", Background: "#0B6B49"},
		Prompt:      Style{Foreground: "#103926", Background: "#D8B94A", Bold: true},
		Warning:     Style{Foreground: "#3A1800", Background: "#F4B183", Bold: true},
		DossierCard: Style{Foreground: "#DFF9EC", Background: "#0E7A53"},
	}
}

func rootBundle() Bundle {
	return Bundle{
		Header:      Style{Foreground: "#FFFFFF", Background: "#7A1421", Bold: true},
		Viewport:    Style{Foreground: "#FCECEE", Background: "#941C2A"},
		Prompt:      Style{Foreground: "#FFFFFF", Background: "#A11E2D", Bold: true},
		Warning:     Style{Foreground: "#2D050A", Background: "#F28A94", Bold: true},
		DossierCard: Style{Foreground: "#FFECEE", Background: "#8A1A27"},
	}
}

func grayscaleBundle() Bundle {
	return Bundle{
		Header:      Style{Foreground: "#FFFFFF", Background: "#111111", Bold: true},
		Viewport:    Style{Foreground: "#F2F2F2", Background: "#1A1A1A"},
		Prompt:      Style{Foreground: "#FFFFFF", Background: "#000000", Bold: true},
		Warning:     Style{Foreground: "#000000", Background: "#E6E6E6", Bold: true},
		DossierCard: Style{Foreground: "#FFFFFF", Background: "#222222"},
	}
}

func highContrastBundle(variant Variant) Bundle {
	bundle := grayscaleBundle()
	// Maintain semantic accent cues in monochrome mode.
	switch variant {
	case VariantWest, VariantRoot, VariantFitra, VariantRead, VariantArchive:
		return bundle
	default:
		return bundle
	}
}
