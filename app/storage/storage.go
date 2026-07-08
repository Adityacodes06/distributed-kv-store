package storage

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"distributed-kv-store/app/models"

	_ "modernc.org/sqlite"
)

// WriteAheadLog manages the append-only log of mutations
type WriteAheadLog struct {
	path    string
	lock    sync.Mutex
	entries []models.LogEntry
}

// NewWriteAheadLog creates or loads a WAL from dataDir
func NewWriteAheadLog(dataDir string) (*WriteAheadLog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	path := filepath.Join(dataDir, "wal.jsonl")
	wal := &WriteAheadLog{
		path:    path,
		entries: []models.LogEntry{},
	}
	if err := wal.load(); err != nil {
		return nil, fmt.Errorf("failed to load WAL: %w", err)
	}
	return wal, nil
}

func (w *WriteAheadLog) load() error {
	if _, err := os.Stat(w.path); os.IsNotExist(err) {
		return nil
	}
	file, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var entry models.LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return err
		}
		w.entries = append(w.entries, entry)
	}
	return scanner.Err()
}

func (w *WriteAheadLog) persist(entry models.LogEntry) error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}

func (w *WriteAheadLog) lastIndexUnsafe() int {
	if len(w.entries) == 0 {
		return -1
	}
	return w.entries[len(w.entries)-1].Index
}

// LastIndex returns the last log entry index
func (w *WriteAheadLog) LastIndex() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.lastIndexUnsafe()
}

// Append creates and persists a new LogEntry
func (w *WriteAheadLog) Append(operation, key, value string) (models.LogEntry, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	index := len(w.entries)
	entry := models.LogEntry{
		Index:     index,
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	if err := w.persist(entry); err != nil {
		return models.LogEntry{}, err
	}
	w.entries = append(w.entries, entry)
	return entry, nil
}

// AppendEntry appends an existing LogEntry if its index is newer
func (w *WriteAheadLog) AppendEntry(entry models.LogEntry) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if entry.Index <= w.lastIndexUnsafe() {
		return nil
	}

	if err := w.persist(entry); err != nil {
		return err
	}
	w.entries = append(w.entries, entry)
	return nil
}

// EntriesAfter returns all log entries with index > afterIndex
func (w *WriteAheadLog) EntriesAfter(afterIndex int) []models.LogEntry {
	w.lock.Lock()
	defer w.lock.Unlock()

	var result []models.LogEntry
	for _, entry := range w.entries {
		if entry.Index > afterIndex {
			result = append(result, entry)
		}
	}
	return result
}

// AllEntries returns all entries in the WAL
func (w *WriteAheadLog) AllEntries() []models.LogEntry {
	w.lock.Lock()
	defer w.lock.Unlock()

	result := make([]models.LogEntry, len(w.entries))
	copy(result, w.entries)
	return result
}

// KVStore manages Key-Value queries using SQLite
type KVStore struct {
	db   *sql.DB
	lock sync.Mutex
}

// NewKVStore creates or opens the SQLite database in dataDir
func NewKVStore(dataDir string) (*KVStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	dbPath := filepath.Join(dataDir, "kv.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	kv := &KVStore{db: db}
	if err := kv.createTable(); err != nil {
		db.Close()
		return nil, err
	}
	return kv, nil
}

func (k *KVStore) Close() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	return k.db.Close()
}

func (k *KVStore) createTable() error {
	_, err := k.db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
	return err
}

// Get retrieves the value for key
func (k *KVStore) Get(key string) (string, bool, error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	var value string
	err := k.db.QueryRow("SELECT value FROM kv WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// Put inserts or updates a key-value pair
func (k *KVStore) Put(key, value string) error {
	k.lock.Lock()
	defer k.lock.Unlock()

	_, err := k.db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
	return err
}

// Delete removes a key-value pair
func (k *KVStore) Delete(key string) (bool, error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	res, err := k.db.Exec("DELETE FROM kv WHERE key = ?", key)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// ApplyEntry applies a LogEntry operation to the SQLite DB
func (k *KVStore) ApplyEntry(entry models.LogEntry) error {
	if entry.Operation == "put" {
		return k.Put(entry.Key, entry.Value)
	} else if entry.Operation == "delete" {
		_, err := k.Delete(entry.Key)
		return err
	}
	return fmt.Errorf("unknown operation: %s", entry.Operation)
}

// StorageEngine wraps the WriteAheadLog and SQLite KVStore
type StorageEngine struct {
	wal *WriteAheadLog
	kv  *KVStore
}

// NewStorageEngine creates and loads both WAL and KVStore, then replays the WAL
func NewStorageEngine(dataDir string) (*StorageEngine, error) {
	wal, err := NewWriteAheadLog(dataDir)
	if err != nil {
		return nil, err
	}
	kv, err := NewKVStore(dataDir)
	if err != nil {
		return nil, err
	}

	engine := &StorageEngine{
		wal: wal,
		kv:  kv,
	}
	if err := engine.replayWAL(); err != nil {
		kv.Close()
		return nil, err
	}
	return engine, nil
}

func (s *StorageEngine) Close() error {
	return s.kv.Close()
}

func (s *StorageEngine) replayWAL() error {
	for _, entry := range s.wal.AllEntries() {
		if err := s.kv.ApplyEntry(entry); err != nil {
			return fmt.Errorf("failed to replay entry %d: %w", entry.Index, err)
		}
	}
	return nil
}

// Write appends to WAL and applies to SQLite
func (s *StorageEngine) Write(operation, key, value string) (models.LogEntry, error) {
	entry, err := s.wal.Append(operation, key, value)
	if err != nil {
		return models.LogEntry{}, err
	}
	if err := s.kv.ApplyEntry(entry); err != nil {
		return models.LogEntry{}, err
	}
	return entry, nil
}

// ApplyReplicatedEntry appends to WAL and applies to SQLite
func (s *StorageEngine) ApplyReplicatedEntry(entry models.LogEntry) error {
	if err := s.wal.AppendEntry(entry); err != nil {
		return err
	}
	return s.kv.ApplyEntry(entry)
}

// Read gets a key from the SQLite KVStore
func (s *StorageEngine) Read(key string) (string, bool, error) {
	return s.kv.Get(key)
}

// LogIndex returns the latest WAL index
func (s *StorageEngine) LogIndex() int {
	return s.wal.LastIndex()
}

// GetWAL returns the underlying WAL
func (s *StorageEngine) GetWAL() *WriteAheadLog {
	return s.wal
}
