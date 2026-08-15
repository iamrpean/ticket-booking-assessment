package booking

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/iamrpean/ticket-booking-assessment/internal/store"
)

// 100 pembeli rebutan 1 tiket VIP tersisa: harus tepat 1 yang berhasil,
// sisanya ditolak karena habis, stok akhir 0, dan cuma ada 1 baris purchase.
func TestBuy_SatuTiketBanyakPembeli(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := store.SeedTicket(db, "vip-1", "VIP", 1); err != nil {
		t.Fatal(err)
	}

	svc := &Service{DB: db}
	const pembeli = 100

	var wg sync.WaitGroup
	var mu sync.Mutex
	sukses, habis := 0, 0
	var tak []error

	for i := 0; i < pembeli; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Buy(context.Background(), "vip-1", fmt.Sprintf("user-%d", i), 500)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				sukses++
			case errors.Is(err, ErrSoldOut):
				habis++
			default:
				tak = append(tak, err)
			}
		}(i)
	}
	wg.Wait()

	if len(tak) > 0 {
		t.Fatalf("ada %d error tak terduga, contoh: %v", len(tak), tak[0])
	}
	if sukses != 1 {
		t.Fatalf("harusnya tepat 1 pembelian sukses, dapat %d", sukses)
	}
	if habis != pembeli-1 {
		t.Fatalf("harusnya %d ditolak karena habis, dapat %d", pembeli-1, habis)
	}

	var stock, rows int
	if err := db.QueryRow(`SELECT stock FROM tickets WHERE id = 'vip-1'`).Scan(&stock); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM purchases`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if stock != 0 || rows != 1 {
		t.Fatalf("stok akhir = %d (harusnya 0), purchases = %d (harusnya 1)", stock, rows)
	}
}

// Scenario 3: pembelian sukses harus meninggalkan baris outbox PENDING
// dalam transaksi yang sama - modal si dispatcher untuk kirim ke accounting.
func TestBuy_MencatatOutbox(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := store.SeedTicket(db, "vip-1", "VIP", 1); err != nil {
		t.Fatal(err)
	}

	svc := &Service{DB: db}
	p, err := svc.Buy(context.Background(), "vip-1", "andi", 500)
	if err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(1) FROM outbox WHERE kind = 'purchase'
		AND ref_id = ? AND status = 'PENDING'`, fmt.Sprint(p.ID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("harusnya ada 1 baris outbox PENDING untuk purchase %d, dapat %d", p.ID, n)
	}

	// pembelian gagal (stok habis) tidak boleh menambah outbox
	_, _ = svc.Buy(context.Background(), "vip-1", "budi", 500)
	if err := db.QueryRow(`SELECT COUNT(1) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pembelian gagal ikut menulis outbox: total %d baris", n)
	}
}

func TestBuy_TiketTidakAda(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := &Service{DB: db}
	if _, err := svc.Buy(context.Background(), "ghost", "user-1", 500); !errors.Is(err, ErrNotFound) {
		t.Fatalf("harusnya ErrNotFound, dapat %v", err)
	}
}
