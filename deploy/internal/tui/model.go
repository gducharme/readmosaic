package tui

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mosaic-terminal/internal/theme"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/rivo/uniseg"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/bidi"
)

// Architecture:
//   - Pane 1: fixed header (protocol/node, live status, observer identifier, active vector).
//   - Pane 2: bounded viewport buffer with append/replace streaming contract.
//   - Pane 3: interactive prompt with blink cursor in command mode.
//
// State machine:
//
//	MOTD -> TRIAGE -> COMMAND -> EXIT
//	- MOTD only accepts Enter (all other keys are no-op).
//	- TRIAGE supports A/B/C and optional Esc back to MOTD only before command mode is entered.
//	- COMMAND never re-enters MOTD.
//
// Tick system:
//   - status ticks toggle STATUS: [LIVE] visibility in TTY mode.
//   - cursor ticks toggle prompt cursor visibility in TTY mode.
//   - scheduling is external to this model; each tick message mutates state exactly once.
const (
	statusLiveOn  = "STATUS: [LIVE]"
	statusLiveOff = "STATUS: [    ]"
	promptPrefix  = "MSC-USER ~ $ "

	defaultStatusTick       = 450 * time.Millisecond
	defaultCursorTick       = 530 * time.Millisecond
	defaultTypewriterTick   = 32 * time.Millisecond
	defaultTypewriterBatch  = 1
	maxTypewriterQueueLines = 1024
	maxViewportLines        = 512
	defaultReadFragmentPath = "internal/content/vector_a_read_fragment.txt"
	readFragmentPathEnvVar  = "MOSAIC_VECTOR_A_FRAGMENT_PATH"
	archiveRootEnvVar       = "MOSAIC_ARCHIVE_ROOT"
	archiveSeedEnvVar       = "MOSAIC_ARCHIVE_SEED_DEFAULTS"
	defaultArchiveRoot      = "/archive"
	typewriterTickMsEnvVar  = "MOSAIC_TUI_TYPEWRITER_TICK_MS"
	typewriterBatchEnvVar   = "MOSAIC_TUI_TYPEWRITER_BATCH"
	readFallbackLine        = "READ FRAGMENT UNAVAILABLE"
	typewriterQueueDropLine = "[TYPEWRITER QUEUE TRUNCATED]"
	maxArchiveFileBytes     = 2 * 1024 * 1024
	menuSelectionDebounce   = 75 * time.Millisecond
	flowArchive             = "archive"
	flowVector              = "vector"
	flowDefault             = "default"
)

// Keybindings (source of truth for tests and operators):
//
//	+---------+----------------------------------+
//	| Mode    | Keys                             |
//	+---------+----------------------------------+
//	| MOTD    | enter                            |
//	| TRIAGE  | a/A, b/B, c/C, esc (pre-command) |
//	| COMMAND | text, backspace, enter, ctrl+d   |
//	+---------+----------------------------------+

// Screen identifies the active top-level TUI view.
type Screen int

const (
	ScreenMOTD Screen = iota
	ScreenTriage
	ScreenCommand
	ScreenArchiveLanguage
	ScreenArchiveFile
	ScreenArchiveEditor
	ScreenExit
)

// TickSource exposes blink cadence for deterministic tests.
type TickSource interface {
	StatusTick() time.Duration
	CursorTick() time.Duration
	TypewriterTick() time.Duration
}

type defaultTickSource struct{}

func (defaultTickSource) StatusTick() time.Duration { return defaultStatusTick }
func (defaultTickSource) CursorTick() time.Duration { return defaultCursorTick }
func (defaultTickSource) TypewriterTick() time.Duration {
	if v, ok := readPositiveIntFromEnv(typewriterTickMsEnvVar); ok {
		return time.Duration(v) * time.Millisecond
	}
	return defaultTypewriterTick
}

// Message types consumed by Update.
type (
	TickMsg           struct{}
	CursorTickMsg     struct{}
	TypewriterTickMsg struct{}
	KeyMsg            struct{ Key string }
	ResizeMsg         struct{ Width, Height int }
)

// ExternalEvent is a minimal interface for runtime-fed events (logs/status updates).
type ExternalEvent interface {
	Apply(*Model)
}

// AppendLineMsg appends a line to pane-2 viewport stream.
type AppendLineMsg struct{ Line string }

// Apply implements ExternalEvent.
func (m AppendLineMsg) Apply(model *Model) { model.appendViewportLine(m.Line) }

// ReplaceViewportMsg replaces pane-2 viewport content.
type ReplaceViewportMsg struct{ Content string }

// Apply implements ExternalEvent.
func (m ReplaceViewportMsg) Apply(model *Model) { model.setViewportContent(m.Content) }

// StatusUpdateMsg injects status content into the viewport stream.
type StatusUpdateMsg struct{ Status string }

// Apply implements ExternalEvent.
func (m StatusUpdateMsg) Apply(model *Model) { model.appendViewportLine("STATUS UPDATE: " + m.Status) }

// Options controls model defaults and environment behavior.
type Options struct {
	Width          int
	Height         int
	IsTTY          bool
	MaxBufferLines int
	Ticks          TickSource
	ThemeBundle    *theme.Bundle
	TypewriterStep int
	Username       string
	// Flow selects the startup UI flow (e.g. "archive").
	Flow string
}

type archiveLanguage struct {
	DirName     string
	DisplayCode string
	DisplayName string
	Path        string
	IsRTL       bool
}

type archiveDocument struct {
	Name string
	Path string
}

type archiveSeedFile struct {
	Name    string
	Content string
}

type archiveSeedLanguage struct {
	DirName string
	Files   []archiveSeedFile
}

var archiveSeedLanguages = []archiveSeedLanguage{
	{
		DirName: "en",
		Files: []archiveSeedFile{
			{Name: "001-Intro", Content: "Welcome to the archive.\n"},
			{Name: "002-The start", Content: "This is the start of the story.\n"},
			{Name: "003-The surprise", Content: "A surprise appears at the end.\n"},
		},
	},
	{
		DirName: "fr",
		Files: []archiveSeedFile{
			{Name: "001-Intro", Content: "Bienvenue dans les archives.\n"},
			{Name: "002-Le début", Content: "Voici le début de l'histoire.\n"},
			{Name: "003-La surprise", Content: "Une surprise apparaît à la fin.\n"},
		},
	},
	{
		DirName: "ar",
		Files: []archiveSeedFile{
			{Name: "001-Intro", Content: "مرحباً بك في الأرشيف.\n"},
			{Name: "002-البداية", Content: "هذه هي بداية القصة.\n"},
			{Name: "003-المفاجأة", Content: "تظهر مفاجأة في النهاية.\n"},
		},
	},
}

// Model represents the terminal UI state with three panes.
type Model struct {
	width  int
	height int
	isTTY  bool

	viewportLines []string
	viewportTop   int
	viewportH     int
	maxBuffer     int

	statusBlink bool
	cursorBlink bool

	screen            Screen
	hasEnteredCommand bool
	observerHash      string
	promptInput       string
	selectedVector    string
	ticks             TickSource
	themeBundle       theme.Bundle
	hasThemeBundle    bool
	typewriterQueue   []string
	typewriterActive  bool
	typewriterTarget  []string
	typewriterCursor  int
	typewriterLineIdx int // -1 indicates no active animated line
	typewriterRTL     bool
	typewriterStep    int

	username                string
	archiveRoot             string
	archiveLanguages        []archiveLanguage
	archiveFiles            []archiveDocument
	archiveLanguageIdx      int
	archiveFileIdx          int
	archiveEditPath         string
	archiveEditorBuffer     string
	archiveCursor           int
	archiveStatus           string
	menuLastSelectionKey    string
	menuLastSelectionAt     time.Time
	menuLastSelectionScreen Screen
	now                     func() time.Time
	archiveReadOnly         bool
}

// NewModel constructs the interactive TUI model from caller/session metadata.
func NewModel(remoteAddr string, width, height int) Model {
	return NewModelWithOptions(remoteAddr, Options{Width: width, Height: height, IsTTY: true})
}

// NewModelWithOptions builds a model with explicit runtime options.
func NewModelWithOptions(remoteAddr string, opts Options) Model {
	maxBuffer := opts.MaxBufferLines
	if maxBuffer <= 0 {
		maxBuffer = maxViewportLines
	}
	ticks := opts.Ticks
	if ticks == nil {
		ticks = defaultTickSource{}
	}

	m := Model{
		width:          max(opts.Width, 1),
		height:         max(opts.Height, 1),
		viewportH:      computeViewportHeight(opts.Height),
		isTTY:          opts.IsTTY,
		statusBlink:    true,
		cursorBlink:    true,
		screen:         ScreenMOTD,
		observerHash:   deriveObserverHash(remoteAddr),
		maxBuffer:      maxBuffer,
		ticks:          ticks,
		viewportLines:  strings.Split(renderMOTD(), "\n"),
		typewriterStep: resolveTypewriterStep(opts.TypewriterStep),
		username:       strings.ToLower(strings.TrimSpace(opts.Username)),
		now:            time.Now,
	}
	flow := normalizeFlow(opts.Flow)
	if opts.ThemeBundle != nil {
		m.themeBundle = cloneThemeBundle(*opts.ThemeBundle)
		m.hasThemeBundle = true
	}
	if !m.isTTY {
		m.statusBlink = true
		m.cursorBlink = false
	}
	m.enforceBufferLimit()
	m.clampViewportBounds()
	if isArchiveMode(m.username, flow) {
		m.initArchiveMode()
	}
	return m
}

func normalizeFlow(flow string) string {
	normalized := strings.ToLower(strings.TrimSpace(flow))
	switch normalized {
	case "", flowDefault:
		return ""
	case flowArchive:
		return flowArchive
	case flowVector:
		return flowVector
	default:
		return ""
	}
}

// isArchiveMode resolves startup precedence for archive UX.
//
// NOTE: server flow and UI mode intentionally diverge for root-family users:
// server keeps them on vector routing, while the TUI applies a read-only
// archive UI override when flow=vector for those identities.
// Explicit archive flow takes precedence, then username fallback preserves
// backward compatibility for archive user sessions when flow is unset/default.
func isArchiveMode(username, flow string) bool {
	normalizedFlow := normalizeFlow(flow)
	if normalizedFlow == flowArchive {
		return true
	}
	if normalizedFlow == flowVector && isArchiveReadOnlyUser(username) {
		return true
	}
	return isArchiveDefaultUser(username)
}

func isArchiveDefaultUser(username string) bool {
	return strings.EqualFold(strings.TrimSpace(username), "archive")
}

func isArchiveReadOnlyUser(username string) bool {
	switch strings.ToLower(strings.TrimSpace(username)) {
	case "root", "fitra", "west":
		return true
	default:
		return false
	}
}

// NextStatusTick returns blink cadence.
func (m Model) NextStatusTick() time.Duration { return m.ticks.StatusTick() }

// NextCursorTick returns prompt cursor blink cadence.
func (m Model) NextCursorTick() time.Duration { return m.ticks.CursorTick() }

// NextTypewriterTick returns readout animation cadence.
func (m Model) NextTypewriterTick() time.Duration { return m.ticks.TypewriterTick() }

// Observer identity rule: hash host only (no port) to reduce churn across ephemeral source ports.

// Update advances model state in response to events.
func (m Model) Update(msg any) Model {
	switch msg := msg.(type) {
	case ResizeMsg:
		m.width = max(msg.Width, 1)
		m.height = max(msg.Height, 1)
		m.viewportH = computeViewportHeight(m.height)
		m.clampViewportBounds()
	case TickMsg:
		if m.isTTY {
			m.statusBlink = !m.statusBlink
		}
	case CursorTickMsg:
		if m.isTTY {
			m.cursorBlink = !m.cursorBlink
		}
	case TypewriterTickMsg:
		m.advanceTypewriter()
	case KeyMsg:
		m.handleKey(msg.Key)
	case ExternalEvent:
		msg.Apply(&m)
	}

	m.clampViewportBounds()
	return m
}

func (m *Model) handleKey(key string) {
	lower := strings.ToLower(key)
	if m.shouldSuppressPostMenuDuplicate(lower) {
		return
	}
	switch m.screen {
	case ScreenMOTD:
		if lower == "enter" {
			m.screen = ScreenTriage
			m.setViewportContent(renderTriageMenu())
		}
	case ScreenTriage:
		if lower == "esc" && !m.hasEnteredCommand {
			m.screen = ScreenMOTD
			m.setViewportContent(renderMOTD())
			return
		}
		if m.shouldSuppressMenuSelectionKey(lower) {
			return
		}
		m.selectVectorByKey(lower)
	case ScreenCommand:
		if m.typewriterActive || len(m.typewriterQueue) > 0 {
			m.flushTypewriter()
		}
		switch lower {
		case "ctrl+d":
			m.screen = ScreenExit
			m.appendViewportLine("SESSION EXIT REQUESTED")
		case "enter":
			line := strings.TrimSpace(m.promptInput)
			m.promptInput = ""
			if line != "" {
				m.appendViewportLine(promptPrefix + line)
			}
		case "backspace":
			runes := []rune(m.promptInput)
			if len(runes) > 0 {
				m.promptInput = string(runes[:len(runes)-1])
			}
		default:
			runes := []rune(key)
			if len(runes) == 1 {
				m.promptInput += key
			}
		}
	case ScreenExit:
		// no-op
	case ScreenArchiveLanguage:
		m.handleArchiveLanguageKey(lower, key)
	case ScreenArchiveFile:
		m.handleArchiveFileKey(lower, key)
	case ScreenArchiveEditor:
		m.handleArchiveEditorKey(lower, key)
	}
}

func (m *Model) initArchiveMode() {
	m.archiveRoot = strings.TrimSpace(os.Getenv(archiveRootEnvVar))
	if m.archiveRoot == "" {
		m.archiveRoot = defaultArchiveRoot
	}
	m.screen = ScreenArchiveLanguage
	m.archiveReadOnly = isArchiveReadOnlyUser(m.username)
	m.promptInput = ""
	m.selectedVector = "ARCHIVE"
	if archiveSeedEnabled() {
		m.ensureArchiveSeedContent()
	}
	m.loadArchiveLanguages()
	m.renderArchiveLanguageMenu()
}

func archiveSeedEnabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(archiveSeedEnvVar)))
	if value == "" {
		return true
	}
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func (m *Model) ensureArchiveSeedContent() {
	root := filepath.Clean(m.archiveRoot)
	for _, lang := range archiveSeedLanguages {
		langDir := filepath.Join(root, lang.DirName)
		if err := os.MkdirAll(langDir, 0o755); err != nil {
			m.archiveStatus = fmt.Sprintf("Failed to prepare archive directory %s: %v", lang.DirName, err)
			continue
		}
		for _, seed := range lang.Files {
			path := filepath.Join(langDir, seed.Name)
			if _, err := os.Stat(path); err == nil {
				continue
			} else if !os.IsNotExist(err) {
				m.archiveStatus = fmt.Sprintf("Failed to inspect archive file %s: %v", path, err)
				continue
			}
			if err := os.WriteFile(path, []byte(seed.Content), 0o600); err != nil {
				m.archiveStatus = fmt.Sprintf("Failed to seed archive file %s: %v", path, err)
			}
		}
	}
}

func (m *Model) loadArchiveLanguages() {
	entries, err := os.ReadDir(filepath.Clean(m.archiveRoot))
	if err != nil {
		m.archiveLanguages = nil
		m.archiveStatus = fmt.Sprintf("Archive root unavailable: %v", err)
		return
	}

	langs := make([]archiveLanguage, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		display := archiveDirectoryToLanguageCode(dirName)
		menuName := archiveLanguageMenuName(display, dirName)
		langs = append(langs, archiveLanguage{DirName: dirName, DisplayCode: display, DisplayName: menuName, Path: filepath.Join(m.archiveRoot, dirName), IsRTL: isRTLLanguage(display)})
	}
	sort.Slice(langs, func(i, j int) bool {
		return strings.ToLower(langs[i].DirName) < strings.ToLower(langs[j].DirName)
	})
	m.archiveLanguages = langs
	m.archiveStatus = ""
}

func archiveDirectoryToLanguageCode(dirName string) string {
	normalized := strings.TrimSpace(dirName)
	if normalized == "" {
		return "und"
	}

	tag, err := language.Parse(normalized)
	if err == nil {
		return tag.String()
	}

	fallback := map[string]string{
		"english":  "en",
		"francais": "fr",
		"français": "fr",
		"arabic":   "ar",
	}
	key := strings.ToLower(normalized)
	if code, ok := fallback[key]; ok {
		return code
	}
	return normalized
}

func archiveLanguageAutonym(code string) string {
	base := strings.ToLower(strings.Split(strings.TrimSpace(code), "-")[0])
	autonyms := map[string]string{
		"en": "English",
		"fr": "Français",
		"ar": "العربية",
	}
	if name, ok := autonyms[base]; ok {
		return name
	}
	if base == "" {
		return code
	}
	return strings.ToUpper(base)
}

func archiveLanguageMenuName(code, dirName string) string {
	autonym := archiveLanguageAutonym(code)
	if strings.EqualFold(strings.TrimSpace(dirName), strings.TrimSpace(code)) {
		return fmt.Sprintf("%s (%s)", autonym, code)
	}
	return fmt.Sprintf("%s (%s · dir: %s)", autonym, code, dirName)
}

func isRTLLanguage(code string) bool {
	base := strings.ToLower(strings.Split(strings.TrimSpace(code), "-")[0])
	switch base {
	case "ar", "fa", "he", "ur", "ps", "dv", "yi", "ku", "syr":
		return true
	default:
		return false
	}
}

func (m *Model) renderArchiveLanguageMenu() {
	lines := []string{fmt.Sprintf("ARCHIVE ROOT: %s", m.archiveRoot), "LANGUAGE MENU // USE INTERNATIONAL CODES"}
	if m.archiveStatus != "" {
		lines = append(lines, "", "WARNING: "+m.archiveStatus)
	}
	if len(m.archiveLanguages) == 0 {
		lines = append(lines, "", "No language directories found.")
	} else {
		for idx, lang := range m.archiveLanguages {
			lines = append(lines, fmt.Sprintf("%d) %s -> %s", idx+1, lang.DisplayName, filepath.Join(m.archiveRoot, lang.DirName)))
		}
	}
	lines = append(lines, "", "Type language number then Enter.")
	m.setViewportContent(strings.Join(lines, "\n"))
}

func (m *Model) shouldSuppressPostMenuDuplicate(key string) bool {
	if m.menuLastSelectionScreen == m.screen {
		return false
	}
	if !isMenuSelectionKeyForScreen(m.menuLastSelectionScreen, key) {
		return false
	}
	now := time.Now()
	if m.now != nil {
		now = m.now()
	}
	if key != m.menuLastSelectionKey || m.menuLastSelectionAt.IsZero() {
		return false
	}
	if now.Sub(m.menuLastSelectionAt) > menuSelectionDebounce {
		return false
	}
	m.menuLastSelectionAt = now
	return true
}

func (m *Model) handleArchiveLanguageKey(lower, raw string) {
	switch lower {
	case "ctrl+d":
		m.screen = ScreenExit
		return
	case "backspace":
		m.clearMenuSelectionDebounceState()
		if len(m.promptInput) > 0 {
			r := []rune(m.promptInput)
			m.promptInput = string(r[:len(r)-1])
		}
		return
	case "enter":
		m.clearMenuSelectionDebounceState()
		choice, err := strconv.Atoi(strings.TrimSpace(m.promptInput))
		m.promptInput = ""
		if err != nil || choice <= 0 || choice > len(m.archiveLanguages) {
			return
		}
		m.archiveLanguageIdx = choice - 1
		m.loadArchiveFiles(m.archiveLanguages[m.archiveLanguageIdx])
		m.screen = ScreenArchiveFile
		m.renderArchiveFileMenu()
		return
	}
	m.appendMenuSelectionDigit(raw)
}

func (m *Model) loadArchiveFiles(lang archiveLanguage) {
	entries, err := os.ReadDir(filepath.Clean(lang.Path))
	if err != nil {
		m.archiveFiles = nil
		m.archiveStatus = fmt.Sprintf("Cannot read language directory: %v", err)
		return
	}
	files := make([]archiveDocument, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, archiveDocument{Name: entry.Name(), Path: filepath.Join(lang.Path, entry.Name())})
	}
	sort.Slice(files, func(i, j int) bool { return strings.ToLower(files[i].Name) < strings.ToLower(files[j].Name) })
	m.archiveFiles = files
	m.archiveStatus = ""
}

func resolvePathWithSymlinks(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absPath)
	if err == nil {
		return filepath.Clean(resolved), nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	parent := filepath.Dir(absPath)
	resolvedParent, parentErr := filepath.EvalSymlinks(parent)
	if parentErr != nil {
		return "", parentErr
	}
	return filepath.Clean(filepath.Join(resolvedParent, filepath.Base(absPath))), nil
}

// NOTE: Path resolution reduces traversal/symlink escapes but cannot fully eliminate
// TOCTOU races on a concurrently mutating filesystem. Deployments assume archive
// root write access is trusted and controlled by operators.
func (m *Model) isPathInsideArchiveRoot(candidate string) bool {
	resolvedRoot, err := resolvePathWithSymlinks(filepath.Clean(m.archiveRoot))
	if err != nil {
		return false
	}
	resolvedCandidate, err := resolvePathWithSymlinks(filepath.Clean(candidate))
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(resolvedRoot, resolvedCandidate)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func (m *Model) renderArchiveFileMenu() {
	lang := m.archiveLanguages[m.archiveLanguageIdx]
	lines := []string{fmt.Sprintf("ARCHIVE LANGUAGE: %s", lang.DisplayName)}
	if m.archiveStatus != "" {
		lines = append(lines, "WARNING: "+m.archiveStatus)
	}
	if len(m.archiveFiles) == 0 {
		lines = append(lines, "", "No files found for this language.")
	} else {
		for idx, file := range m.archiveFiles {
			lines = append(lines, fmt.Sprintf("%d) %s", idx+1, file.Name))
		}
	}
	lines = append(lines, "", "Type file number then Enter. Esc returns to languages.")
	m.setViewportContent(strings.Join(lines, "\n"))
}

func (m *Model) handleArchiveFileKey(lower, raw string) {
	switch lower {
	case "ctrl+d":
		m.screen = ScreenExit
		return
	case "esc":
		m.clearMenuSelectionDebounceState()
		m.promptInput = ""
		m.screen = ScreenArchiveLanguage
		m.renderArchiveLanguageMenu()
		return
	case "backspace":
		m.clearMenuSelectionDebounceState()
		if len(m.promptInput) > 0 {
			r := []rune(m.promptInput)
			m.promptInput = string(r[:len(r)-1])
		}
		return
	case "enter":
		m.clearMenuSelectionDebounceState()
		choice, err := strconv.Atoi(strings.TrimSpace(m.promptInput))
		m.promptInput = ""
		if err != nil || choice <= 0 || choice > len(m.archiveFiles) {
			return
		}
		m.archiveFileIdx = choice - 1
		m.openArchiveFile(m.archiveFiles[m.archiveFileIdx])
		m.screen = ScreenArchiveEditor
		if m.archiveReadOnly {
			m.renderArchiveReadOnlyFile(true)
		} else {
			m.renderArchiveEditor()
		}
		return
	}
	m.appendMenuSelectionDigit(raw)
}

func (m *Model) shouldSuppressMenuSelectionKey(key string) bool {
	if !isMenuSelectionKeyForScreen(m.screen, key) {
		return false
	}
	now := time.Now()
	if m.now != nil {
		now = m.now()
	}
	if m.menuLastSelectionScreen != m.screen {
		m.clearMenuSelectionDebounceState()
	}
	if key == m.menuLastSelectionKey && !m.menuLastSelectionAt.IsZero() && now.Sub(m.menuLastSelectionAt) <= menuSelectionDebounce {
		m.menuLastSelectionAt = now
		m.menuLastSelectionScreen = m.screen
		return true
	}
	m.menuLastSelectionKey = key
	m.menuLastSelectionAt = now
	m.menuLastSelectionScreen = m.screen
	return false
}

func isMenuSelectionKeyForScreen(screen Screen, key string) bool {
	switch screen {
	case ScreenTriage:
		return key == "a" || key == "b" || key == "c"
	case ScreenArchiveLanguage, ScreenArchiveFile:
		r := []rune(key)
		return len(r) == 1 && unicode.IsDigit(r[0])
	default:
		return false
	}
}

func (m *Model) appendMenuSelectionDigit(raw string) {
	runes := []rune(raw)
	if len(runes) != 1 || !unicode.IsDigit(runes[0]) {
		return
	}
	if m.shouldSuppressMenuSelectionKey(raw) {
		return
	}
	m.promptInput += raw
}

func (m *Model) clearMenuSelectionDebounceState() {
	m.menuLastSelectionKey = ""
	m.menuLastSelectionAt = time.Time{}
	m.menuLastSelectionScreen = m.screen
}

func sanitizeArchiveLoadedContent(content string) string {
	builder := strings.Builder{}
	for _, r := range content {
		if r == '\n' || r == '\t' {
			builder.WriteRune(r)
			continue
		}
		if unicode.IsControl(r) {
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func (m *Model) openArchiveFile(file archiveDocument) {
	clean := filepath.Clean(file.Path)
	if !m.isPathInsideArchiveRoot(clean) {
		m.archiveEditorBuffer = ""
		m.archiveStatus = "Refusing to open file outside archive root"
		return
	}
	info, err := os.Stat(clean)
	if err != nil {
		m.archiveEditorBuffer = ""
		m.archiveStatus = fmt.Sprintf("Unable to stat %s: %v", file.Name, err)
		return
	}
	if info.Size() > maxArchiveFileBytes {
		m.archiveEditorBuffer = ""
		m.archiveStatus = fmt.Sprintf("Refusing to open %s: file too large (%d bytes > %d)", file.Name, info.Size(), maxArchiveFileBytes)
		return
	}
	data, err := os.ReadFile(clean)
	if err != nil {
		m.archiveEditorBuffer = ""
		m.archiveStatus = fmt.Sprintf("Unable to open %s: %v", file.Name, err)
		return
	}
	m.archiveEditPath = clean
	m.archiveEditorBuffer = sanitizeArchiveLoadedContent(string(data))
	m.archiveCursor = len([]rune(m.archiveEditorBuffer))
	m.archiveStatus = ""
}

func (m *Model) renderArchiveReadOnlyFile(animate bool) {
	m.archiveCursor = clamp(m.archiveCursor, 0, len([]rune(m.archiveEditorBuffer)))
	lang := m.archiveLanguages[m.archiveLanguageIdx]
	file := m.archiveFiles[m.archiveFileIdx]
	dir := "LTR"
	if lang.IsRTL {
		dir = "RTL"
	}
	lines := []string{
		fmt.Sprintf("ARCHIVE EDITOR // %s", file.Name),
		fmt.Sprintf("Language: %s [%s]", lang.DisplayName, dir),
		"READ-ONLY VIEW. Typewriter playback enabled.",
		"Esc returns to file list.",
		"",
	}
	if m.archiveStatus != "" {
		lines = append(lines, "WARNING: "+m.archiveStatus, "")
	}
	m.setViewportContent(strings.Join(lines, "\n"))
	if !animate {
		return
	}
	content := strings.Split(m.archiveEditorBuffer, "\n")
	// Typewriter lines are appended to viewport content; the static header above
	// remains visible while body lines animate in.
	m.enqueueTypewriter(content...)
}

func (m *Model) renderArchiveEditor() {
	m.archiveCursor = clamp(m.archiveCursor, 0, len([]rune(m.archiveEditorBuffer)))
	lang := m.archiveLanguages[m.archiveLanguageIdx]
	file := m.archiveFiles[m.archiveFileIdx]
	dir := "LTR"
	if lang.IsRTL {
		dir = "RTL"
	}
	line, col := archiveCursorLineCol(m.archiveEditorBuffer, m.archiveCursor)
	modeLine := "Arrow/Home/End navigation enabled. Edits save immediately."
	if m.archiveReadOnly {
		modeLine = "READ-ONLY VIEW. Typewriter playback enabled."
	}
	lines := []string{fmt.Sprintf("ARCHIVE EDITOR // %s", file.Name), fmt.Sprintf("Language: %s [%s]", lang.DisplayName, dir), modeLine, fmt.Sprintf("Cursor: Ln %d, Col %d", line, col), "Esc returns to file list.", ""}
	if m.archiveStatus != "" {
		lines = append(lines, "WARNING: "+m.archiveStatus, "")
	}
	if m.archiveReadOnly {
		lines = append(lines, strings.Split(m.archiveEditorBuffer, "\n")...)
	} else {
		lines = append(lines, renderArchiveBufferWithCursor(m.archiveEditorBuffer, m.archiveCursor, m.cursorBlink)...)
	}
	m.setViewportContent(strings.Join(lines, "\n"))
}

func (m *Model) handleArchiveEditorKey(lower, raw string) {
	m.archiveCursor = clamp(m.archiveCursor, 0, len([]rune(m.archiveEditorBuffer)))
	if m.archiveReadOnly {
		switch lower {
		case "ctrl+d":
			m.screen = ScreenExit
			return
		case "esc":
			m.promptInput = ""
			m.screen = ScreenArchiveFile
			m.renderArchiveFileMenu()
			return
		default:
			m.archiveStatus = "Read-only mode: edits are disabled"
			m.renderArchiveReadOnlyFile(false)
			return
		}
	}

	switch lower {
	case "ctrl+d":
		m.screen = ScreenExit
		return
	case "esc":
		m.promptInput = ""
		m.screen = ScreenArchiveFile
		m.renderArchiveFileMenu()
		return
	case "backspace":
		runes := []rune(m.archiveEditorBuffer)
		if m.archiveCursor > 0 && len(runes) > 0 {
			start := runes[:m.archiveCursor-1]
			end := runes[m.archiveCursor:]
			m.archiveEditorBuffer = string(append(start, end...))
			m.archiveCursor--
			m.persistArchiveEdit()
		}
		m.renderArchiveEditor()
		return
	case "enter":
		m.archiveEditorBuffer = insertArchiveRunesAtCursor(m.archiveEditorBuffer, m.archiveCursor, []rune("\n"))
		m.archiveCursor++
		m.persistArchiveEdit()
		m.renderArchiveEditor()
		return
	case "left":
		if m.archiveCursor > 0 {
			m.archiveCursor--
		}
		m.renderArchiveEditor()
		return
	case "right":
		if m.archiveCursor < len([]rune(m.archiveEditorBuffer)) {
			m.archiveCursor++
		}
		m.renderArchiveEditor()
		return
	case "up":
		m.archiveCursor = moveArchiveCursorVertical(m.archiveEditorBuffer, m.archiveCursor, -1)
		m.renderArchiveEditor()
		return
	case "down":
		m.archiveCursor = moveArchiveCursorVertical(m.archiveEditorBuffer, m.archiveCursor, 1)
		m.renderArchiveEditor()
		return
	case "home":
		m.archiveCursor = moveArchiveCursorHome(m.archiveEditorBuffer, m.archiveCursor)
		m.renderArchiveEditor()
		return
	case "end":
		m.archiveCursor = moveArchiveCursorEnd(m.archiveEditorBuffer, m.archiveCursor)
		m.renderArchiveEditor()
		return
	}
	if raw != "" {
		filtered := strings.Builder{}
		for _, r := range raw {
			if unicode.IsControl(r) {
				continue
			}
			filtered.WriteRune(r)
		}
		if filtered.Len() == 0 {
			return
		}
		payload := []rune(filtered.String())
		m.archiveEditorBuffer = insertArchiveRunesAtCursor(m.archiveEditorBuffer, m.archiveCursor, payload)
		m.archiveCursor += len(payload)
		m.persistArchiveEdit()
		m.renderArchiveEditor()
	}
}

func insertArchiveRunesAtCursor(buffer string, cursor int, insert []rune) string {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	merged := make([]rune, 0, len(runes)+len(insert))
	merged = append(merged, runes[:cursor]...)
	merged = append(merged, insert...)
	merged = append(merged, runes[cursor:]...)
	return string(merged)
}

func archiveLineStarts(runes []rune) []int {
	starts := []int{0}
	for i, r := range runes {
		if r == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

func archiveLineForCursor(starts []int, cursor int) int {
	line := 0
	for i := 1; i < len(starts); i++ {
		if starts[i] > cursor {
			break
		}
		line = i
	}
	return line
}

func archiveLineEnd(runes []rune, starts []int, line int) int {
	if line+1 < len(starts) {
		return starts[line+1] - 1
	}
	return len(runes)
}

func moveArchiveCursorVertical(buffer string, cursor, delta int) int {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	starts := archiveLineStarts(runes)
	line := archiveLineForCursor(starts, cursor)
	col := cursor - starts[line]
	target := clamp(line+delta, 0, len(starts)-1)
	targetEnd := archiveLineEnd(runes, starts, target)
	targetLen := targetEnd - starts[target]
	if targetLen < 0 {
		targetLen = 0
	}
	return starts[target] + min(col, targetLen)
}

func moveArchiveCursorHome(buffer string, cursor int) int {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	starts := archiveLineStarts(runes)
	line := archiveLineForCursor(starts, cursor)
	return starts[line]
}

func moveArchiveCursorEnd(buffer string, cursor int) int {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	starts := archiveLineStarts(runes)
	line := archiveLineForCursor(starts, cursor)
	return archiveLineEnd(runes, starts, line)
}

func archiveCursorLineCol(buffer string, cursor int) (int, int) {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	starts := archiveLineStarts(runes)
	line := archiveLineForCursor(starts, cursor)
	return line + 1, cursor - starts[line] + 1
}

func renderArchiveBufferWithCursor(buffer string, cursor int, blinkOn bool) []string {
	runes := []rune(buffer)
	cursor = clamp(cursor, 0, len(runes))
	glyph := "░"
	if blinkOn {
		glyph = "█"
	}
	decorated := make([]rune, 0, len(runes)+1)
	decorated = append(decorated, runes[:cursor]...)
	decorated = append(decorated, []rune(glyph)...)
	decorated = append(decorated, runes[cursor:]...)
	return strings.Split(string(decorated), "\n")
}

func (m *Model) persistArchiveEdit() {
	if m.archiveEditPath == "" {
		return
	}
	clean := filepath.Clean(m.archiveEditPath)
	if !m.isPathInsideArchiveRoot(clean) {
		m.archiveStatus = "Save failed: refusing to write outside archive root"
		return
	}
	payload := []byte(m.archiveEditorBuffer)
	if len(payload) > maxArchiveFileBytes {
		m.archiveStatus = fmt.Sprintf("Save failed: content exceeds max size (%d bytes > %d)", len(payload), maxArchiveFileBytes)
		return
	}
	info, err := os.Stat(clean)
	if err != nil {
		m.archiveStatus = fmt.Sprintf("Save failed: %v", err)
		return
	}
	tmpFile, err := os.CreateTemp(filepath.Dir(clean), filepath.Base(clean)+".tmp-*")
	if err != nil {
		m.archiveStatus = fmt.Sprintf("Save failed: %v", err)
		return
	}
	_ = tmpFile.Chmod(info.Mode().Perm())
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(payload); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		m.archiveStatus = fmt.Sprintf("Save failed: %v", err)
		return
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		m.archiveStatus = fmt.Sprintf("Save failed: %v", err)
		return
	}
	if err := os.Rename(tmpPath, clean); err != nil {
		_ = os.Remove(tmpPath)
		m.archiveStatus = fmt.Sprintf("Save failed: %v", err)
		return
	}
	m.archiveStatus = "Saved at " + time.Now().Format("15:04:05")
}

func (m *Model) selectVectorByKey(key string) {
	switch key {
	case "a":
		m.activateTriageSelection("VECTOR_A", "READ", loadReadFragmentLines())
	case "b":
		m.activateTriageSelection("VECTOR_B", "ARCHIVE", nil)
	case "c":
		m.activateTriageSelection("VECTOR_C", "RETURN", nil)
	}
}

// Typewriter state machine:
//   - idle: queue empty, no active line.
//   - queue populated: lines enqueued via enqueueTypewriter.
//   - active: one line is progressively revealed across ticks.
//   - draining: current line completes, next queued line becomes active.
//   - idle: all queued lines rendered/flushed.
func (m *Model) activateTriageSelection(vector, mode string, readPayload []string) {
	m.selectedVector = vector
	m.screen = ScreenCommand
	m.hasEnteredCommand = true
	m.setViewportContent(fmt.Sprintf("TRIAGE SELECTION: %s => %s", mode, vector))
	lines := []string{"CONFIRMED VECTOR: " + vector}
	if len(readPayload) > 0 {
		lines = append(lines, "READ PAYLOAD:")
		lines = append(lines, readPayload...)
	}
	lines = append(lines, "Awaiting command input.")
	m.enqueueTypewriter(lines...)
}

func (m *Model) enqueueTypewriter(lines ...string) {
	if len(lines) == 0 {
		return
	}
	m.typewriterQueue = append(m.typewriterQueue, lines...)
	m.enforceTypewriterQueueLimit()
	if !m.typewriterActive {
		m.beginNextTypewriterLine()
	}
}

func (m *Model) enforceTypewriterQueueLimit() {
	if len(m.typewriterQueue) <= maxTypewriterQueueLines {
		return
	}

	// TODO: promote this limit to a runtime setting if operators need larger buffered animations.
	over := len(m.typewriterQueue) - maxTypewriterQueueLines
	m.typewriterQueue = m.typewriterQueue[over:]
	if len(m.typewriterQueue) > 0 {
		m.typewriterQueue[0] = typewriterQueueDropLine
	}
}

func toGraphemeClusters(line string) []string {
	gr := uniseg.NewGraphemes(line)
	clusters := make([]string, 0, len(line))
	for gr.Next() {
		clusters = append(clusters, gr.Str())
	}
	if len(clusters) == 0 {
		return []string{""}
	}
	return clusters
}

func lineHasRTLScript(line string) bool {
	for _, r := range line {
		// LookupRune always returns a Unicode BiDi class; the width return value is
		// not needed for rune-based iteration.
		props, _ := bidi.LookupRune(r)
		switch props.Class() {
		case bidi.R, bidi.AL:
			return true
		case bidi.L:
			return false
		}
	}
	return false
}

func (m *Model) resetTypewriterState() {
	m.typewriterActive = false
	m.typewriterTarget = nil
	m.typewriterCursor = 0
	m.typewriterLineIdx = -1
	m.typewriterRTL = false
}

func (m *Model) flushTypewriter() {
	if m.typewriterActive && m.typewriterLineIdx >= 0 && m.typewriterLineIdx < len(m.viewportLines) {
		m.viewportLines[m.typewriterLineIdx] = strings.Join(m.typewriterTarget, "")
	}
	for len(m.typewriterQueue) > 0 {
		m.appendViewportLineNow(m.typewriterQueue[0])
		m.typewriterQueue = m.typewriterQueue[1:]
	}
	m.resetTypewriterState()
}

func (m *Model) beginNextTypewriterLine() {
	if len(m.typewriterQueue) == 0 {
		m.resetTypewriterState()
		return
	}

	line := m.typewriterQueue[0]
	m.typewriterQueue = m.typewriterQueue[1:]
	m.appendViewportLineNow("")
	m.typewriterActive = true
	m.typewriterTarget = toGraphemeClusters(line)
	m.typewriterCursor = 0
	m.typewriterLineIdx = len(m.viewportLines) - 1
	m.typewriterRTL = lineHasRTLScript(line)
}

func (m *Model) advanceTypewriter() {
	if !m.typewriterActive {
		if len(m.typewriterQueue) == 0 {
			return
		}
		m.beginNextTypewriterLine()
	}

	if !m.typewriterActive || m.typewriterLineIdx < 0 || m.typewriterLineIdx >= len(m.viewportLines) {
		return
	}

	if m.typewriterCursor < len(m.typewriterTarget) {
		step := max(m.typewriterStep, 1)
		m.typewriterCursor = min(m.typewriterCursor+step, len(m.typewriterTarget))
		// Keep reveal in logical (prefix) order for all scripts.
		// RTL display order should be handled by the terminal renderer's BiDi support.
		m.viewportLines[m.typewriterLineIdx] = strings.Join(m.typewriterTarget[:m.typewriterCursor], "")
	}

	if m.typewriterCursor >= len(m.typewriterTarget) {
		m.resetTypewriterState()
	}
}

func loadReadFragmentLines() []string {
	path := strings.TrimSpace(os.Getenv(readFragmentPathEnvVar))
	if path == "" {
		path = defaultReadFragmentPath
	}

	content, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return []string{readFallbackLine}
	}

	fragment := strings.TrimSpace(string(content))
	if fragment == "" {
		return []string{readFallbackLine}
	}

	lines := strings.Split(fragment, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return lines
}

// Render returns a pure string representation of the current model.
func Render(m Model) string {
	return strings.Join([]string{
		renderHeader(m),
		renderViewport(m),
		renderPrompt(m),
	}, "\n")
}

// View method delegates to pure Render.
func (m Model) View() string { return Render(m) }

func renderHeader(m Model) string {
	status := statusLiveOff
	if m.statusBlink {
		status = statusLiveOn
	}
	vector := "NONE"
	if m.selectedVector != "" {
		vector = m.selectedVector
	}
	head := strings.Join([]string{
		"MOSAIC PROTOCOL v.1.0 // NODE: GENESIS_BLOCK",
		status,
		fmt.Sprintf("OBSERVER: [%s]", m.observerHash),
		fmt.Sprintf("VECTOR: [%s]", vector),
	}, "\n")
	if !m.isTTY || !m.hasThemeBundle {
		return head
	}
	return applyStyle(head, m.themeBundle.Header)
}

func renderViewport(m Model) string {
	if len(m.viewportLines) == 0 || m.viewportH == 0 {
		return ""
	}
	from := m.viewportTop
	to := min(from+m.viewportH, len(m.viewportLines))
	if from >= to {
		return ""
	}
	content := strings.Join(m.viewportLines[from:to], "\n")
	if !m.isTTY || !m.hasThemeBundle {
		return content
	}
	return applyStyle(content, m.themeBundle.Viewport)
}

func renderPrompt(m Model) string {
	var prompt string
	switch m.screen {
	case ScreenExit:
		prompt = promptPrefix + "[SESSION CLOSED]"
	case ScreenMOTD:
		prompt = promptPrefix + "[PRESS ENTER TO CONTINUE]"
	case ScreenTriage:
		prompt = promptPrefix + "[PRESS A/B/C TO SELECT, ESC TO RETURN]"
	case ScreenCommand:
		cursor := " "
		if m.cursorBlink {
			cursor = "█"
		}
		prompt = promptPrefix + m.promptInput + cursor
	case ScreenArchiveLanguage:
		prompt = promptPrefix + "[ARCHIVE LANGUAGE #] " + m.promptInput
	case ScreenArchiveFile:
		prompt = promptPrefix + "[ARCHIVE FILE #] " + m.promptInput
	case ScreenArchiveEditor:
		if m.archiveReadOnly {
			fileName := ""
			if m.archiveFileIdx >= 0 && m.archiveFileIdx < len(m.archiveFiles) {
				fileName = m.archiveFiles[m.archiveFileIdx].Name
			}
			if fileName != "" {
				prompt = promptPrefix + "[READ ONLY " + fileName + "]"
			} else {
				prompt = promptPrefix + "[READ ONLY]"
			}
		} else {
			cursor := " "
			if m.cursorBlink {
				cursor = "█"
			}
			marker := "◌"
			if m.cursorBlink {
				marker = "◉"
			}
			prompt = promptPrefix + "[EDITING LIVE " + marker + "]" + cursor
		}
	default:
		prompt = promptPrefix
	}

	if !m.isTTY || !m.hasThemeBundle {
		return prompt
	}
	return applyStyle(prompt, m.themeBundle.Prompt)
}

func cloneThemeBundle(src theme.Bundle) theme.Bundle {
	return theme.Bundle{
		StyleSet: theme.StyleSet{
			Header:      src.Header,
			Viewport:    src.Viewport,
			Prompt:      src.Prompt,
			Warning:     src.Warning,
			DossierCard: src.DossierCard,
		},
		Roles: src.Roles,
	}
}

func applyStyle(content string, style theme.Style) string {
	parts := []string{}
	if fg := rgbFromHex(style.Foreground); fg != "" {
		parts = append(parts, "38;2;"+fg)
	}
	if bg := rgbFromHex(style.Background); bg != "" {
		parts = append(parts, "48;2;"+bg)
	}
	if style.Bold {
		parts = append(parts, "1")
	}
	if len(parts) == 0 {
		return content
	}

	prefix := "\x1b[" + strings.Join(parts, ";") + "m"
	reset := "\x1b[0m"
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		lines[i] = prefix + line + reset
	}
	return strings.Join(lines, "\n")
}

func rgbFromHex(v string) string {
	v = strings.TrimSpace(strings.TrimPrefix(v, "#"))
	if len(v) != 6 {
		return ""
	}
	r, err := strconv.ParseInt(v[0:2], 16, 64)
	if err != nil {
		return ""
	}
	g, err := strconv.ParseInt(v[2:4], 16, 64)
	if err != nil {
		return ""
	}
	b, err := strconv.ParseInt(v[4:6], 16, 64)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%d;%d;%d", r, g, b)
}

func (m *Model) setViewportContent(content string) {
	m.viewportLines = strings.Split(content, "\n")
	m.viewportTop = 0
	m.enforceBufferLimit()
	m.clampViewportBounds()
}

func (m *Model) appendViewportLine(line string) {
	if m.typewriterActive || len(m.typewriterQueue) > 0 {
		m.enqueueTypewriter(line)
		return
	}
	m.appendViewportLineNow(line)
}

func (m *Model) appendViewportLineNow(line string) {
	m.viewportLines = append(m.viewportLines, line)
	m.enforceBufferLimit()
	m.viewportTop = max(len(m.viewportLines)-m.viewportH, 0)
	m.clampViewportBounds()
}

func readPositiveIntFromEnv(name string) (int, bool) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func resolveTypewriterStep(configured int) int {
	if configured > 0 {
		return configured
	}
	if v, ok := readPositiveIntFromEnv(typewriterBatchEnvVar); ok {
		return v
	}
	return defaultTypewriterBatch
}

func (m *Model) enforceBufferLimit() {
	if m.maxBuffer <= 0 || len(m.viewportLines) <= m.maxBuffer {
		return
	}
	over := len(m.viewportLines) - m.maxBuffer
	copy(m.viewportLines, m.viewportLines[over:])
	m.viewportLines = m.viewportLines[:m.maxBuffer]
	m.viewportTop = max(m.viewportTop-over, 0)
}

func (m *Model) clampViewportBounds() {
	m.viewportH = max(m.viewportH, 0)
	maxTop := max(len(m.viewportLines)-m.viewportH, 0)
	m.viewportTop = clamp(m.viewportTop, 0, maxTop)
}

func deriveObserverHash(remoteAddr string) string {
	normalized := normalizeRemoteAddr(remoteAddr)
	sum := sha256.Sum256([]byte(normalized))
	return strings.ToUpper(hex.EncodeToString(sum[:]))[:12]
}

func normalizeRemoteAddr(remoteAddr string) string {
	v := strings.TrimSpace(remoteAddr)
	if v == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(v); err == nil {
		host = strings.Trim(strings.TrimSpace(host), "[]")
		if ip := net.ParseIP(host); ip != nil {
			host = ip.String()
		}
		return host
	}

	host := strings.Trim(v, "[]")
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
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
		"Press Enter to continue.",
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

func computeViewportHeight(totalHeight int) int {
	// Keep the body pane visible even in very small PTYs so MOTD/menus never fully disappear.
	return max(totalHeight-7, 1)
}

func clamp(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}
