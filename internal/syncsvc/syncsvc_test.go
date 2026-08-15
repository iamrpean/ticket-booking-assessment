package syncsvc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/iamrpean/ticket-booking-assessment/internal/testdb"
)

func newService(t *testing.T) *Service {
	t.Helper()
	return &Service{DB: testdb.New(t)}
}

// Skenario assessment persis: update kedua (qty=2, v2) tiba duluan, update
// pertama (qty=5, v1) menyusul karena latency. Hasil akhir harus qty=2.
func TestApply_UpdateTerbalik(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	applied, err := svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 2, Version: 2})
	if err != nil || !applied {
		t.Fatalf("update v2 harusnya diterapkan (applied=%v err=%v)", applied, err)
	}

	applied, err = svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 5, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("update v1 yang telat harusnya ditolak sebagai basi")
	}

	got, err := svc.Get(ctx, "vip-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Quantity != 2 || got.Version != 2 {
		t.Fatalf("harusnya qty=2 v=2, dapat qty=%d v=%d", got.Quantity, got.Version)
	}
}

func TestApply_UrutanNormal(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	if _, err := svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 5, Version: 1}); err != nil {
		t.Fatal(err)
	}
	applied, err := svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 2, Version: 2})
	if err != nil || !applied {
		t.Fatalf("update lebih baru harusnya diterapkan (applied=%v err=%v)", applied, err)
	}

	got, _ := svc.Get(ctx, "vip-1")
	if got.Quantity != 2 || got.Version != 2 {
		t.Fatalf("harusnya qty=2 v=2, dapat qty=%d v=%d", got.Quantity, got.Version)
	}
}

// Version sama dikirim dua kali (delivery duplikat) -> yang kedua diabaikan.
func TestApply_VersionSamaIdempoten(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	if _, err := svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 5, Version: 3}); err != nil {
		t.Fatal(err)
	}
	applied, err := svc.Apply(ctx, Update{TicketID: "vip-1", Quantity: 99, Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("version yang sama harusnya tidak diterapkan ulang")
	}
	got, _ := svc.Get(ctx, "vip-1")
	if got.Quantity != 5 {
		t.Fatalf("quantity berubah oleh duplikat: %d", got.Quantity)
	}
}

// 30 update versi 1..30 datang serentak dalam urutan acak (goroutine
// scheduler): apapun urutannya, akhirnya version tertinggi yang menang.
func TestApply_KonkurenVersiTertinggiMenang(t *testing.T) {
	svc := newService(t)
	const n = 30

	var wg sync.WaitGroup
	for v := int64(1); v <= n; v++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			if _, err := svc.Apply(context.Background(),
				Update{TicketID: "vip-1", Quantity: v * 10, Version: v}); err != nil {
				t.Error(err)
			}
		}(v)
	}
	wg.Wait()

	got, err := svc.Get(context.Background(), "vip-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != n || got.Quantity != n*10 {
		t.Fatalf("harusnya v=%d qty=%d, dapat v=%d qty=%d", n, n*10, got.Version, got.Quantity)
	}
}

func TestApply_Validasi(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Apply(context.Background(), Update{Quantity: 5, Version: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tanpa ticket_id harusnya ErrInvalid, dapat %v", err)
	}
	if _, err := svc.Apply(context.Background(), Update{TicketID: "vip-1", Quantity: 5}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("version 0 harusnya ErrInvalid, dapat %v", err)
	}
}
