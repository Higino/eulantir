package transform

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"github.com/higino/eulantir/internal/connector"
)

// openPlugins is a process-wide registry of loaded plugins keyed by content
// hash. Go's runtime allows each plugin (identified by its build ID) to be
// opened only once per process — attempting to open the same plugin a second
// time, even from a different path, returns "plugin already loaded".
// Storing the *plugin.Plugin here lets every caller re-use the same handle.
var (
	openPlugins   = map[string]*plugin.Plugin{}
	openPluginsMu sync.Mutex
)

// Loader compiles and loads Go plugin transforms.
// Compiled .so files are cached under CacheDir by source-content hash so a
// transform is only rebuilt when its source changes.
type Loader struct {
	// CacheDir is where compiled .so plugins are stored.
	// Defaults to ".eulantir-cache" relative to the working directory.
	CacheDir string

	// ExtraBuildFlags are appended to the "go build" invocation.
	// Set to []string{"-cover"} when running under go test -cover so the
	// plugin is instrumented the same way as the test binary — otherwise Go's
	// plugin version checker rejects the .so with "different version of package".
	ExtraBuildFlags []string
}

// DefaultLoader returns a Loader that caches under .eulantir-cache/.
func DefaultLoader() *Loader {
	return &Loader{CacheDir: ".eulantir-cache"}
}

// Load compiles sourcePath into a Go plugin (if not already cached), opens it,
// and returns a Transform that calls the exported function named by entrypoint.
//
// The exported function in the plugin must have the signature:
//
//	func Entrypoint(ctx context.Context, in connector.Record) ([]connector.Record, error)
//
// sourcePath may be relative (resolved against the working directory) or absolute.
func (l *Loader) Load(name, sourcePath, entrypoint string) (Transform, error) {
	// --- resolve absolute source path ---
	absSrc, err := filepath.Abs(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("resolve source path %q: %w", sourcePath, err)
	}

	src, err := os.ReadFile(absSrc)
	if err != nil {
		return nil, fmt.Errorf("read transform source %q: %w", absSrc, err)
	}

	// --- cache path = <cacheDir>/<name>_<hash12>.so ---
	hash := fmt.Sprintf("%x", sha256.Sum256(src))[:12]
	cacheDir := l.CacheDir
	if cacheDir == "" {
		cacheDir = ".eulantir-cache"
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir %q: %w", cacheDir, err)
	}
	pluginPath, err := filepath.Abs(filepath.Join(cacheDir, fmt.Sprintf("%s_%s.so", name, hash)))
	if err != nil {
		return nil, fmt.Errorf("resolve plugin path: %w", err)
	}

	// --- compile if not on disk ---
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		if err := l.compile(absSrc, pluginPath); err != nil {
			return nil, err
		}
	}

	// --- open plugin (or reuse already-open handle) ---
	openPluginsMu.Lock()
	p, alreadyOpen := openPlugins[hash]
	openPluginsMu.Unlock()

	if !alreadyOpen {
		p, err = plugin.Open(pluginPath)
		if err != nil {
			return nil, fmt.Errorf("open plugin %q: %w", pluginPath, err)
		}
		openPluginsMu.Lock()
		openPlugins[hash] = p
		openPluginsMu.Unlock()
	}

	// --- resolve entrypoint symbol ---
	sym, err := p.Lookup(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("symbol %q not found in plugin %q: %w", entrypoint, pluginPath, err)
	}

	fn, ok := sym.(func(context.Context, connector.Record) ([]connector.Record, error))
	if !ok {
		return nil, fmt.Errorf(
			"symbol %q has wrong type — expected func(context.Context, connector.Record) ([]connector.Record, error)",
			entrypoint,
		)
	}

	return &pluginTransform{name: name, fn: fn}, nil
}

// compile runs "go build -buildmode=plugin -o pluginPath sourceFile".
// The build is executed from the module root so imports are resolved correctly.
func (l *Loader) compile(sourceFile, pluginPath string) error {
	// find the module root (directory containing go.mod nearest to sourceFile)
	moduleRoot, err := findModuleRoot(sourceFile)
	if err != nil {
		// fallback to working directory
		moduleRoot, _ = os.Getwd()
	}

	args := append([]string{"build", "-buildmode=plugin", "-o", pluginPath}, l.ExtraBuildFlags...)
	args = append(args, sourceFile)
	cmd := exec.Command("go", args...)
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("compile transform %q:\n%s\n%w", sourceFile, strings.TrimSpace(string(out)), err)
	}
	return nil
}

// findModuleRoot walks up from dir until it finds a directory containing go.mod.
func findModuleRoot(path string) (string, error) {
	dir := filepath.Dir(path)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %q", path)
		}
		dir = parent
	}
}

// pluginTransform wraps a compiled plugin function as a Transform.
type pluginTransform struct {
	name string
	fn   func(context.Context, connector.Record) ([]connector.Record, error)
}

func (t *pluginTransform) Name() string { return t.name }

func (t *pluginTransform) Apply(ctx context.Context, in connector.Record) ([]connector.Record, error) {
	return t.fn(ctx, in)
}

// Load is a package-level convenience using DefaultLoader.
func Load(name, sourcePath, entrypoint string) (Transform, error) {
	return DefaultLoader().Load(name, sourcePath, entrypoint)
}
