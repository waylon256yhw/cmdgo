package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store is the goroutine-safe handle to the JSON state file. Hold s.mu in
// read mode to inspect, in write mode for any mutation; Update wraps both.
type Store struct {
	path string

	mu sync.RWMutex
	st State
}

// New opens (or creates) the state file at path. The parent directory is
// created with 0700 permissions if missing.
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("store: create data dir: %w", err)
	}
	s := &Store{path: path}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		s.st = State{
			Version:  StateVersion,
			Settings: DefaultSettings(),
		}
		return s.persistLocked()
	}
	if err != nil {
		return fmt.Errorf("store: read %s: %w", s.path, err)
	}
	if len(data) == 0 {
		s.st = State{Version: StateVersion, Settings: DefaultSettings()}
		return s.persistLocked()
	}
	if err := json.Unmarshal(data, &s.st); err != nil {
		return fmt.Errorf("store: decode %s: %w", s.path, err)
	}
	if s.st.Version == 0 {
		s.st.Version = StateVersion
	}
	// Fill in any defaults absent from an older / hand-edited file. We only
	// rewrite when something was actually missing so unrelated edits to the
	// JSON survive verbatim.
	dirty := false
	if s.st.Settings == (Settings{}) {
		s.st.Settings = DefaultSettings()
		dirty = true
	} else {
		if s.st.Settings.Routing == "" {
			s.st.Settings.Routing = "affinity"
			dirty = true
		}
		if s.st.Settings.TrafficLogMax <= 0 {
			s.st.Settings.TrafficLogMax = 500
			dirty = true
		}
		if s.st.Settings.CreditPollSec <= 0 {
			s.st.Settings.CreditPollSec = 60
			dirty = true
		}
	}
	if dirty {
		return s.persistLocked()
	}
	return nil
}

// persistLocked writes the current in-memory state to disk atomically. The
// caller must hold s.mu (either branch — load uses it before any goroutine
// can race with us).
func (s *Store) persistLocked() error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.json.tmp")
	if err != nil {
		return fmt.Errorf("store: create tmp: %w", err)
	}
	tmpName := tmp.Name()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&s.st); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: encode: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: close tmp: %w", err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: chmod tmp: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

// Path returns the absolute path of the backing state file.
func (s *Store) Path() string { return s.path }

// Snapshot returns a deep copy of the current state safe to use outside
// the store's lock.
func (s *Store) Snapshot() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.Clone()
}

// Update runs fn with exclusive write access to the state and persists
// the result atomically. If fn returns an error the state is rolled back
// to the value before fn ran and nothing is written to disk.
func (s *Store) Update(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.st.Clone()
	if err := fn(&s.st); err != nil {
		s.st = prev
		return err
	}
	if err := s.persistLocked(); err != nil {
		s.st = prev
		return err
	}
	return nil
}
