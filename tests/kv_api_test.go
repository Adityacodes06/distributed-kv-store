package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-kv-store/app/config"
	"distributed-kv-store/app/models"
	"distributed-kv-store/app/server"
	"distributed-kv-store/app/storage"
)

func newTestServer(t *testing.T) (*httptest.Server, *server.KVNode) {
	t.Helper()
	dataDir := setupTempDir(t)

	cfg := &config.Config{
		NodeID:    "test-node",
		NodeRole:  "leader",
		NodeHost:  "127.0.0.1",
		DataDir:   dataDir,
		Peers:     []string{},
		RawPeers:  []string{},
		LeaderURL: "http://127.0.0.1:0",
	}

	engine, err := storage.NewStorageEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create storage engine: %v", err)
	}
	t.Cleanup(func() {
		engine.Close()
	})

	node := server.NewKVNode(cfg, engine)
	mux := node.SetupMux()
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return ts, node
}

func TestHTTP_Health(t *testing.T) {
	ts, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var health models.HealthReply
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}

	if health.Status != "healthy" || health.NodeID != "test-node" || health.Role != "leader" {
		t.Errorf("unexpected health body: %+v", health)
	}
}

func TestHTTP_MetricsInitial(t *testing.T) {
	ts, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	var metrics models.MetricsReply
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode metrics response: %v", err)
	}

	if metrics.TotalReads != 0 || metrics.TotalWrites != 0 || metrics.LogIndex != -1 {
		t.Errorf("unexpected initial metrics: %+v", metrics)
	}
}

func TestHTTP_PutAndGet(t *testing.T) {
	ts, _ := newTestServer(t)

	// PUT
	reqBody := `{"value":"Aditya"}`
	req, _ := http.NewRequest("PUT", ts.URL+"/kv/name", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected PUT 200, got %d", resp.StatusCode)
	}

	var putReply models.PutReply
	json.NewDecoder(resp.Body).Decode(&putReply)
	if putReply.Message != "stored" || putReply.Key != "name" || putReply.Value != "Aditya" {
		t.Errorf("unexpected PUT response: %+v", putReply)
	}

	// GET
	respGet, err := http.Get(ts.URL + "/kv/name")
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer respGet.Body.Close()

	if respGet.StatusCode != http.StatusOK {
		t.Errorf("expected GET 200, got %d", respGet.StatusCode)
	}

	var getReply models.GetReply
	json.NewDecoder(respGet.Body).Decode(&getReply)
	if getReply.Value != "Aditya" || getReply.Key != "name" {
		t.Errorf("unexpected GET response: %+v", getReply)
	}
}

func TestHTTP_GetMissingKey(t *testing.T) {
	ts, _ := newTestServer(t)

	resp, err := http.Get(ts.URL + "/kv/nonexistent")
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected GET 404, got %d", resp.StatusCode)
	}
}

func TestHTTP_PutOverwrite(t *testing.T) {
	ts, _ := newTestServer(t)

	// First PUT
	req1, _ := http.NewRequest("PUT", ts.URL+"/kv/color", strings.NewReader(`{"value":"red"}`))
	http.DefaultClient.Do(req1)

	// Second PUT
	req2, _ := http.NewRequest("PUT", ts.URL+"/kv/color", strings.NewReader(`{"value":"blue"}`))
	resp2, _ := http.DefaultClient.Do(req2)
	resp2.Body.Close()

	// GET
	respGet, _ := http.Get(ts.URL + "/kv/color")
	defer respGet.Body.Close()

	var getReply models.GetReply
	json.NewDecoder(respGet.Body).Decode(&getReply)
	if getReply.Value != "blue" {
		t.Errorf("expected overwritten value blue, got %s", getReply.Value)
	}
}

func TestHTTP_Delete(t *testing.T) {
	ts, _ := newTestServer(t)

	// PUT
	req1, _ := http.NewRequest("PUT", ts.URL+"/kv/temp", strings.NewReader(`{"value":"123"}`))
	res1, _ := http.DefaultClient.Do(req1)
	res1.Body.Close()

	// DELETE
	req2, _ := http.NewRequest("DELETE", ts.URL+"/kv/temp", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected DELETE 200, got %d", resp2.StatusCode)
	}

	var deleteReply models.DeleteReply
	json.NewDecoder(resp2.Body).Decode(&deleteReply)
	if deleteReply.Message != "deleted" || deleteReply.Key != "temp" || deleteReply.Value != nil {
		t.Errorf("unexpected DELETE reply: %+v", deleteReply)
	}

	// GET (should be 404)
	respGet, _ := http.Get(ts.URL + "/kv/temp")
	defer respGet.Body.Close()
	if respGet.StatusCode != http.StatusNotFound {
		t.Errorf("expected GET 404 after delete, got %d", respGet.StatusCode)
	}
}

func TestHTTP_DeleteMissingKey(t *testing.T) {
	ts, _ := newTestServer(t)

	// DELETE
	req, _ := http.NewRequest("DELETE", ts.URL+"/kv/ghost", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected DELETE 200 for missing key, got %d", resp.StatusCode)
	}
}

func TestHTTP_MetricsAfterOperations(t *testing.T) {
	ts, _ := newTestServer(t)

	// 2 PUTs, 1 GET, 1 DELETE
	req1, _ := http.NewRequest("PUT", ts.URL+"/kv/a", strings.NewReader(`{"value":"1"}`))
	http.DefaultClient.Do(req1)
	req2, _ := http.NewRequest("PUT", ts.URL+"/kv/b", strings.NewReader(`{"value":"2"}`))
	http.DefaultClient.Do(req2)

	http.Get(ts.URL + "/kv/a")

	req3, _ := http.NewRequest("DELETE", ts.URL+"/kv/b", nil)
	http.DefaultClient.Do(req3)

	resp, _ := http.Get(ts.URL + "/metrics")
	defer resp.Body.Close()

	var metrics models.MetricsReply
	json.NewDecoder(resp.Body).Decode(&metrics)

	if metrics.TotalWrites != 2 {
		t.Errorf("expected 2 writes in metrics, got %d", metrics.TotalWrites)
	}
	if metrics.TotalReads < 1 {
		t.Errorf("expected at least 1 read in metrics, got %d", metrics.TotalReads)
	}
	if metrics.TotalDeletes != 1 {
		t.Errorf("expected 1 delete in metrics, got %d", metrics.TotalDeletes)
	}
	if metrics.LogIndex < 2 {
		t.Errorf("expected log index >= 2, got %d", metrics.LogIndex)
	}
}
