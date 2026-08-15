package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iamrpean/ticket-booking-assessment/internal/testdb"
)

func openDB(t *testing.T) *Service {
	t.Helper()
	svc := New(testdb.New(t), 4096, 200, 20*time.Millisecond)
	t.Cleanup(svc.Close)
	return svc
}

func TestSubmit_TersimpanDenganIDOtomatis(t *testing.T) {
	svc := openDB(t)

	id, err := svc.Submit(context.Background(), Transaction{TicketID: "vip-1", UserID: "andi", Amount: 500})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("id harusnya dibuatkan otomatis")
	}

	var n int
	if err := svc.db.QueryRow(`SELECT COUNT(1) FROM transactions WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("transaksi %s tidak ditemukan setelah Submit kembali", id)
	}
}

// Scenario 3: tiap transaksi yang tersimpan harus punya baris outbox
// PENDING dari transaksi DB yang sama.
func TestSubmit_MencatatOutbox(t *testing.T) {
	svc := openDB(t)

	for i := 0; i < 3; i++ {
		if _, err := svc.Submit(context.Background(), Transaction{
			TicketID: "vip-1", UserID: fmt.Sprintf("u%d", i), Amount: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var n int
	if err := svc.db.QueryRow(`SELECT COUNT(1) FROM outbox
		WHERE kind = 'transaction' AND status = 'PENDING'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("harusnya 3 baris outbox, dapat %d", n)
	}
}

// Skenario assessment: >10.000 transaksi dalam waktu singkat, semua yang
// dijawab sukses harus benar-benar ada di database.
func TestSubmit_10RibuSemuaTersimpan(t *testing.T) {
	svc := openDB(t)
	const total = 10_000

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Submit(context.Background(), Transaction{
				ID:       fmt.Sprintf("tx-%05d", i),
				TicketID: "vip-1",
				UserID:   fmt.Sprintf("user-%d", i),
				Amount:   100,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	elapsed := time.Since(start)

	for err := range errs {
		t.Fatalf("ada submit yang gagal: %v", err)
	}

	var n int
	if err := svc.db.QueryRow(`SELECT COUNT(1) FROM transactions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != total {
		t.Fatalf("tersimpan %d dari %d transaksi", n, total)
	}
	t.Logf("%d transaksi persisten dalam %v (%.0f tx/detik)", n, elapsed, float64(n)/elapsed.Seconds())
}

// Kalau satu baris dalam batch merusak transaksi DB (contoh: ID duplikat),
// seluruh batch gagal dan SEMUA waiter di batch itu diberi tahu - tidak ada
// yang dijawab sukses padahal tidak tersimpan.
func TestSubmit_BatchGagalSemuaDapatError(t *testing.T) {
	db := testdb.New(t)
	// batch kecil + flush lama supaya dua submit di bawah pasti sebatch
	svc := New(db, 16, 2, time.Second)
	t.Cleanup(svc.Close)

	var wg sync.WaitGroup
	errsMu := sync.Mutex{}
	var errs []error
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.Submit(context.Background(), Transaction{ID: "sama", TicketID: "vip-1", UserID: "andi", Amount: 100})
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
		}()
	}
	wg.Wait()

	if errs[0] == nil || errs[1] == nil {
		t.Fatalf("dua-duanya harus dapat error, dapat: %v", errs)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(1) FROM transactions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("tidak boleh ada baris tersimpan dari batch gagal, ada %d", n)
	}
}
