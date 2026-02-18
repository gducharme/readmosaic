package theme

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Variant identifies the thematic palette/style family.
type Variant string

const (
	VariantWest    Variant = "west"
	VariantFitra   Variant = "fitra"
	VariantRoot    Variant = "root"
	VariantRead    Variant = "read"
	VariantArchive Variant = "archive"
)

// SemanticRoles defines stable semantic color slots used across the UI.
//
// Components should generally depend on these semantic roles rather than
// variant-specific color literals.
type SemanticRoles struct {
	Primary string
	Accent  string
	Muted   string
	Danger  string
	Success string
	Border  string
}

// Style describes presentational attributes for a UI element.
type Style struct {
	Foreground string
	Background string
	Bold       bool
}

// StyleSet provides strongly-typed styles for the primary runtime UI surfaces.
type StyleSet struct {
	Header      Style
	Viewport    Style
	Prompt      Style
	Warning     Style
	DossierCard Style
}

// Bundle contains all display styles needed by the runtime UI surface.
type Bundle struct {
	StyleSet
	Roles SemanticRoles
}

// TermProfile describes terminal rendering capabilities derived from TERM.
type TermProfile struct {
	Colors    int
	TrueColor bool
	IsTTY     bool
}

// TermProfileDetector maps a TERM value to a terminal capability profile.
type TermProfileDetector func(term string) TermProfile

// ErrUnknownVariant is returned when a requested variant is not known.
var ErrUnknownVariant = errors.New("unknown theme variant")

var (
	termProfileCache sync.Map
	knownProfiles    = map[string]TermProfile{
		"dumb":           {Colors: 0, TrueColor: false, IsTTY: false},
		"ansi":           {Colors: 8, TrueColor: false, IsTTY: true},
		"linux":          {Colors: 16, TrueColor: false, IsTTY: true},
		"xterm":          {Colors: 16, TrueColor: false, IsTTY: true},
		"xterm-256color": {Colors: 256, TrueColor: false, IsTTY: true},
		"screen":         {Colors: 8, TrueColor: false, IsTTY: true},
		"tmux":           {Colors: 256, TrueColor: false, IsTTY: true},
		"vt100":          {Colors: 8, TrueColor: false, IsTTY: true},
		"xterm-kitty":    {Colors: 1 << 24, TrueColor: true, IsTTY: true},
		"wezterm":        {Colors: 1 << 24, TrueColor: true, IsTTY: true},
	}
)

var palettes = map[Variant]Bundle{
	VariantWest: {
		StyleSet: StyleSet{
			Header:      Style{Foreground: "#FFFFFF", Background: "#0B1F3A", Bold: true},
			Viewport:    Style{Foreground: "#D7E3F4", Background: "#122A4A"},
			Prompt:      Style{Foreground: "#FFFFFF", Background: "#0F345E", Bold: true},
			Warning:     Style{Foreground: "#FFDDE0", Background: "#5B1F2A", Bold: true},
			DossierCard: Style{Foreground: "#EAF1FB", Background: "#163A63"},
		},
		Roles: SemanticRoles{Primary: "#0B1F3A", Accent: "#0F345E", Muted: "#122A4A", Danger: "#5B1F2A", Success: "#1F6B4A", Border: "#2A4C74"},
	},
	VariantFitra: {
		StyleSet: StyleSet{
			Header:      Style{Foreground: "#1A1A1A", Background: "#D4AF37", Bold: true},
			Viewport:    Style{Foreground: "#D2FFE8", Background: "#0B6B49"},
			Prompt:      Style{Foreground: "#103926", Background: "#D8B94A", Bold: true},
			Warning:     Style{Foreground: "#3A1800", Background: "#F4B183", Bold: true},
			DossierCard: Style{Foreground: "#DFF9EC", Background: "#0E7A53"},
		},
		Roles: SemanticRoles{Primary: "#0B6B49", Accent: "#D4AF37", Muted: "#0E7A53", Danger: "#C65E36", Success: "#1E9E68", Border: "#65A989"},
	},
	VariantRoot: {
		StyleSet: StyleSet{
			Header:      Style{Foreground: "#FFFFFF", Background: "#7A1421", Bold: true},
			Viewport:    Style{Foreground: "#FCECEE", Background: "#941C2A"},
			Prompt:      Style{Foreground: "#FFFFFF", Background: "#A11E2D", Bold: true},
			Warning:     Style{Foreground: "#2D050A", Background: "#F28A94", Bold: true},
			DossierCard: Style{Foreground: "#FFECEE", Background: "#8A1A27"},
		},
		Roles: SemanticRoles{Primary: "#7A1421", Accent: "#A11E2D", Muted: "#8A1A27", Danger: "#C92035", Success: "#5B9B68", Border: "#B95765"},
	},
	VariantRead:    grayscaleBundle(),
	VariantArchive: grayscaleBundle(),
}

var variants = [...]Variant{VariantWest, VariantFitra, VariantRoot, VariantRead, VariantArchive}

// Resolve resolves a concrete style bundle for a variant and TERM value.
//
// For lower-capability terminals (xterm-256color and below), Resolve returns
// a monochrome/high-contrast bundle unless color is explicitly forced.
//
// Example:
//
//	bundle, err := theme.Resolve(theme.VariantWest, os.Getenv("TERM"))
//	if err != nil {
//		return err
//	}
//	ui.Header.SetStyle(bundle.Header)
//	ui.Viewport.SetStyle(bundle.Viewport)
//	ui.Prompt.SetStyle(bundle.Prompt)
//	ui.Warning.SetStyle(bundle.Warning)
//	ui.Dossier.SetStyle(bundle.DossierCard)
func Resolve(variant Variant, term string) (Bundle, error) {
	return resolveWith(variant, ResolveOptions{Term: term}, detectTermProfile)
}

// ResolveWithDetector resolves a bundle using a caller-provided TERM detector.
//
// This is primarily intended for tests and advanced integrations that want
// custom TERM/profile mapping behavior without changing palette logic.
func ResolveWithDetector(variant Variant, opts ResolveOptions, detector TermProfileDetector) (Bundle, error) {
	if detector == nil {
		detector = detectTermProfile
	}
	return resolveWith(variant, opts, detector)
}

// DetectTermProfile maps TERM to a terminal capability profile.
func DetectTermProfile(term string) TermProfile {
	return detectTermProfile(term)
}

// ResolveFromEnv resolves the theme using runtime overrides:
//   - THEME_VARIANT (west|fitra|root|read|archive)
//   - THEME_FORCE_COLOR (boolean)
//   - THEME_FORCE_MONO (boolean)
//
// When THEME_DEBUG is true, the resolved profile and decisions are logged.
func ResolveFromEnv(defaultVariant Variant, term string) (Variant, Bundle, error) {
	variant := defaultVariant
	if v := strings.TrimSpace(os.Getenv("THEME_VARIANT")); v != "" {
		variant = Variant(strings.ToLower(v))
	}

	forceColor := parseBoolEnv("THEME_FORCE_COLOR")
	forceMono := parseBoolEnv("THEME_FORCE_MONO")

	bundle, profile, err := resolveWithProfile(variant, ResolveOptions{
		Term:       term,
		ForceColor: forceColor,
		ForceMono:  forceMono,
	}, detectTermProfile)
	if err != nil {
		return variant, Bundle{}, err
	}

	if parseBoolEnv("THEME_DEBUG") {
		log.Printf("theme: variant=%s term=%q colors=%d truecolor=%t tty=%t forceColor=%t forceMono=%t", variant, term, profile.Colors, profile.TrueColor, profile.IsTTY, forceColor, forceMono)
	}

	return variant, bundle, nil
}

// ResolveOptions controls how a bundle is selected once a TERM profile exists.
type ResolveOptions struct {
	Term       string
	ForceColor bool
	ForceMono  bool
}

func resolveWith(variant Variant, opts ResolveOptions, detector TermProfileDetector) (Bundle, error) {
	bundle, _, err := resolveWithProfile(variant, opts, detector)
	return bundle, err
}

func resolveWithProfile(variant Variant, opts ResolveOptions, detector TermProfileDetector) (Bundle, TermProfile, error) {
	base, ok := palettes[variant]
	if !ok {
		return Bundle{}, TermProfile{}, fmt.Errorf("%w: %s", ErrUnknownVariant, variant)
	}

	term := strings.TrimSpace(opts.Term)
	if term == "" {
		term = os.Getenv("TERM")
	}

	profile := detector(term)
	if shouldUseMonochrome(profile, opts) {
		return cloneBundle(grayscaleBundle()), profile, nil
	}

	if variant == VariantRead || variant == VariantArchive {
		return cloneBundle(grayscaleBundle()), profile, nil
	}

	return cloneBundle(base), profile, nil
}

func parseBoolEnv(key string) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

func shouldUseMonochrome(profile TermProfile, opts ResolveOptions) bool {
	if opts.ForceMono {
		return true
	}
	if opts.ForceColor {
		return false
	}
	if !profile.IsTTY {
		return true
	}
	if !profile.TrueColor && profile.Colors <= 256 {
		return true
	}
	return false
}

func detectTermProfile(term string) TermProfile {
	norm := strings.ToLower(strings.TrimSpace(term))
	if cached, ok := termProfileCache.Load(norm); ok {
		return cached.(TermProfile)
	}

	profile := detectTermProfileUncached(norm)
	termProfileCache.Store(norm, profile)
	return profile
}

func detectTermProfileUncached(norm string) TermProfile {
	if norm == "" {
		return TermProfile{Colors: 0, TrueColor: false, IsTTY: false}
	}

	if p, ok := knownProfiles[norm]; ok {
		return p
	}

	profile := TermProfile{Colors: 16, TrueColor: false, IsTTY: true}
	if strings.Contains(norm, "truecolor") || strings.Contains(norm, "24bit") || strings.Contains(norm, "kitty") || strings.Contains(norm, "wezterm") {
		profile.TrueColor = true
		profile.Colors = 1 << 24
	}
	if strings.Contains(norm, "256") {
		profile.Colors = 256
	}
	if strings.Contains(norm, "dumb") {
		profile = TermProfile{Colors: 0, TrueColor: false, IsTTY: false}
	}
	if strings.Contains(norm, "screen") {
		profile.Colors = 8
	}

	return profile
}

func grayscaleBundle() Bundle {
	return Bundle{
		StyleSet: StyleSet{
			Header:      Style{Foreground: "#FFFFFF", Background: "#111111", Bold: true},
			Viewport:    Style{Foreground: "#F2F2F2", Background: "#1A1A1A"},
			Prompt:      Style{Foreground: "#FFFFFF", Background: "#000000", Bold: true},
			Warning:     Style{Foreground: "#000000", Background: "#E6E6E6", Bold: true},
			DossierCard: Style{Foreground: "#FFFFFF", Background: "#222222"},
		},
		Roles: SemanticRoles{Primary: "#111111", Accent: "#FFFFFF", Muted: "#1A1A1A", Danger: "#E6E6E6", Success: "#CFCFCF", Border: "#8F8F8F"},
	}
}

func cloneBundle(in Bundle) Bundle {
	return in
}
