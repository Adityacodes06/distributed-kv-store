package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"distributed-kv-store/app/models"
	"distributed-kv-store/app/storage"
)

func setupTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "kv-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func TestWriteAheadLog_AppendAndRead(t *testing.T) {
	dataDir := setupTempDir(t)
	wal, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	entry, err := wal.Append("put", "name", "Aditya")
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if entry.Index != 0 {
		t.Errorf("expected index 0, got %d", entry.Index)
	}
	if entry.Operation != "put" {
		t.Errorf("expected operation put, got %s", entry.Operation)
	}
	if entry.Key != "name" {
		t.Errorf("expected key name, got %s", entry.Key)
	}
	if entry.Value != "Aditya" {
		t.Errorf("expected value Aditya, got %s", entry.Value)
	}
}

func TestWriteAheadLog_SequentialIndices(t *testing.T) {
	dataDir := setupTempDir(t)
	wal, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	e1, _ := wal.Append("put", "a", "1")
	e2, _ := wal.Append("put", "b", "2")
	e3, _ := wal.Append("delete", "a", "")

	if e1.Index != 0 || e2.Index != 1 || e3.Index != 2 {
		t.Errorf("expected sequential indices [0, 1, 2], got [%d, %d, %d]", e1.Index, e2.Index, e3.Index)
	}
}

func TestWriteAheadLog_LastIndexEmpty(t *testing.T) {
	dataDir := setupTempDir(t)
	wal, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	if idx := wal.LastIndex(); idx != -1 {
		t.Errorf("expected last index -1 for empty WAL, got %d", idx)
	}
}

func TestWriteAheadLog_EntriesAfter(t *testing.T) {
	dataDir := setupTempDir(t)
	wal, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	wal.Append("put", "a", "1")
	wal.Append("put", "b", "2")
	wal.Append("put", "c", "3")

	entries := wal.EntriesAfter(0)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after index 0, got %d", len(entries))
	}
	if entries[0].Index != 1 || entries[1].Index != 2 {
		t.Errorf("expected entries [1, 2], got [%d, %d]", entries[0].Index, entries[1].Index)
	}
}

func TestWriteAheadLog_PersistenceAcrossRestarts(t *testing.T) {
	dataDir := setupTempDir(t)

	wal1, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL1: %v", err)
	}
	wal1.Append("put", "x", "42")
	wal1.Append("put", "y", "99")

	wal2, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to load WAL2: %v", err)
	}

	if idx := wal2.LastIndex(); idx != 1 {
		t.Errorf("expected last index 1, got %d", idx)
	}

	entries := wal2.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Key != "x" || entries[1].Key != "y" {
		t.Errorf("expected keys [x, y], got [%s, %s]", entries[0].Key, entries[1].Key)
	}
}

func TestWriteAheadLog_IdempotentAppendEntry(t *testing.T) {
	dataDir := setupTempDir(t)
	wal, err := storage.NewWriteAheadLog(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	entry := models.LogEntry{Index: 0, Operation: "put", Key: "k", Value: "v"}
	wal.AppendEntry(entry)
	wal.AppendEntry(entry)

	if len(wal.AllEntries()) != 1 {
		t.Errorf("expected 1 entry (idempotency), got %d", len(wal.AllEntries()))
	}
}

func TestStorageEngine_WriteAndRead(t *testing.T) {
	dataDir := setupTempDir(t)
	engine, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	_, err = engine.Write("put", "color", "blue")
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	val, ok, err := engine.Read("color")
	if err != nil || !ok || val != "blue" {
		t.Errorf("expected value blue, got %s (ok=%t, err=%v)", val, ok, err)
	}
}

func TestStorageEngine_Delete(t *testing.T) {
	dataDir := setupTempDir(t)
	engine, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.Write("put", "temp", "123")
	engine.Write("delete", "temp", "")

	val, ok, err := engine.Read("temp")
	if err != nil || ok || val != "" {
		t.Errorf("expected key to be deleted, got value %s (ok=%t, err=%v)", val, ok, err)
	}
}

func TestStorageEngine_LogIndexTracksWrites(t *testing.T) {
	dataDir := setupTempDir(t)
	engine, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	if idx := engine.LogIndex(); idx != -1 {
		t.Errorf("expected initial log index -1, got %d", idx)
	}

	engine.Write("put", "a", "1")
	if idx := engine.LogIndex(); idx != 0 {
		t.Errorf("expected log index 0, got %d", idx)
	}

	engine.Write("put", "b", "2")
	if idx := engine.LogIndex(); idx != 1 {
		t.Errorf("expected log index 1, got %d", idx)
	}
}

func TestStorageEngine_ApplyReplicatedEntry(t *testing.T) {
	dataDir := setupTempDir(t)
	engine, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	entry := models.LogEntry{Index: 0, Operation: "put", Key: "city", Value: "Mumbai"}
	err = engine.ApplyReplicatedEntry(entry)
	if err != nil {
		t.Fatalf("failed to apply replicated entry: %v", err)
	}

	val, ok, _ := engine.Read("city")
	if !ok || val != "Mumbai" {
		t.Errorf("expected city Mumbai, got %s (ok=%t)", val, ok)
	}
	if idx := engine.LogIndex(); idx != 0 {
		t.Errorf("expected log index 0, got %d", idx)
	}
}

func TestStorageEngine_CrashRecovery(t *testing.T) {
	dataDir := setupTempDir(t)

	engine1, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine1: %v", err)
	}
	engine1.Write("put", "hero", "Batman")
	engine1.Write("put", "villain", "Joker")
	engine1.Write("delete", "villain", "")
	engine1.Close()

	// Re-open/restart from same directory
	engine2, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create engine2: %v", err)
	}
	defer engine2.Close()

	val, ok, _ := engine2.Read("hero")
	if !ok || val != "Batman" {
		t.Errorf("expected hero Batman, got %s (ok=%t)", val, ok)
	}

	_, ok, _ = engine2.Read("villain")
	if ok {
		t.Error("expected villain to be deleted")
	}

	if idx := engine2.LogIndex(); idx != 2 {
		t.Errorf("expected log index 2, got %d", idx)
	}
}

func TestKVStore_Concurrency(t *testing.T) {
	dataDir := setupTempDir(t)
	kvPath := filepath.Join(dataDir, "kv.db")
	_ = os.Remove(kvPath) // start clean

	kv, err := storage.NewKVStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create KVStore: %v", err)
	}
	defer kv.Close()

	// Make sure we can run multiple parallel reads/writes without sqlite locking issues
	done := make(chan bool)
	workers := 10
	for i := 0; i < workers; i++ {
		go func(id int) {
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val := fmt.Sprintf("val-%d-%d", id, j)
				kv.Put(key, val)
				kv.Get(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < workers; i++ {
		<-done
	}
}
