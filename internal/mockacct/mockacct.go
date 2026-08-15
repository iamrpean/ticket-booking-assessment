package mockacct

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

// Mock meniru accounting software pihak ketiga untuk demo Scenario 3:
// membalas 500 sebanyak failFirst kali pertama per idempotency key, baru
// setelah itu menerima. Kiriman dengan key yang sama dihitung sekali.
type Mock struct {
	failFirst int

	mu       sync.Mutex
	attempts map[string]int
	received map[string]json.RawMessage
}

func New(failFirst int) *Mock {
	return &Mock{
		failFirst: failFirst,
		attempts:  map[string]int{},
		received:  map[string]json.RawMessage{},
	}
}

func (m *Mock) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /transaction", m.handleTransaction)
	mux.HandleFunc("GET /stats", m.handleStats)
	return mux
}

func (m *Mock) handleTransaction(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-Idempotency-Key")
	body, _ := io.ReadAll(r.Body)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts[key]++
	if m.attempts[key] <= m.failFirst {
		log.Printf("mock accounting: %s percobaan %d -> 500 (disengaja)", key, m.attempts[key])
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	if _, dup := m.received[key]; !dup {
		m.received[key] = json.RawMessage(body)
	}
	log.Printf("mock accounting: %s diterima", key)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func (m *Mock) handleStats(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"received": len(m.received)})
}
