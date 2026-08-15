package outbox

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iamrpean/ticket-booking-assessment/internal/store"
)

func newDispatcher(t *testing.T, target string, client *http.Client, maxAttempts int) (*Dispatcher, *sql.DB, context.CancelFunc) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	d := &Dispatcher{
		DB:          db,
		TargetURL:   target,
		Client:      client,
		PollEvery:   10 * time.Millisecond,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  200 * time.Millisecond,
		MaxAttempts: maxAttempts,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go d.Run(ctx)
	return d, db, cancel
}

func waitStatus(t *testing.T, db *sql.DB, id int64, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var got string
		if err := db.QueryRow(`SELECT status FROM outbox WHERE id = ?`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout menunggu outbox #%d berstatus %s", id, want)
}

// Pihak ketiga membalas 500 dua kali lalu pulih: baris harus tetap PENDING
// selama gagal, di-retry dengan backoff, dan berakhir SENT tepat satu kali.
func TestDispatcher_Retry500SampaiTerkirim(t *testing.T) {
	var mu sync.Mutex
	attempts, delivered := 0, 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts <= 2 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		delivered++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	_, db, _ := newDispatcher(t, ts.URL, ts.Client(), 8)
	res, err := db.Exec(`INSERT INTO outbox (kind, ref_id, payload) VALUES ('purchase', '1', '{"amount":500}')`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()

	waitStatus(t, db, id, "SENT")

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Fatalf("harusnya 3 kali percobaan http (2 gagal + 1 sukses), dapat %d", attempts)
	}
	if delivered != 1 {
		t.Fatalf("harusnya diterima tepat 1 kali, dapat %d", delivered)
	}
}

// Pihak ketiga mati total: setelah MaxAttempts baris jadi DEAD dan
// dispatcher berhenti mencoba (bukan retry selamanya).
func TestDispatcher_DeadLetterSetelahMaxAttempts(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)

	_, db, _ := newDispatcher(t, ts.URL, ts.Client(), 3)
	res, err := db.Exec(`INSERT INTO outbox (kind, ref_id, payload) VALUES ('purchase', '1', '{}')`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()

	waitStatus(t, db, id, "DEAD")

	mu.Lock()
	sesudahDead := attempts
	mu.Unlock()
	if sesudahDead != 3 {
		t.Fatalf("harusnya berhenti di 3 percobaan, dapat %d", sesudahDead)
	}

	time.Sleep(100 * time.Millisecond) // beberapa siklus poll lagi
	mu.Lock()
	defer mu.Unlock()
	if attempts != sesudahDead {
		t.Fatalf("baris DEAD masih di-retry: %d -> %d", sesudahDead, attempts)
	}
}
