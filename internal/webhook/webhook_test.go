package webhook

import (
	"context"
	"sync"
	"testing"

	"github.com/iamrpean/ticket-booking-assessment/internal/testdb"
)

func newService(t *testing.T) *Service {
	t.Helper()
	return &Service{DB: testdb.New(t)}
}

// Skenario assessment: pihak ketiga mengirim ulang request yang identik di
// waktu yang sama. 50 request serentak dengan payment_id sama: semua harus
// dijawab sukses, tepat 1 yang benar-benar insert, dan hanya 1 baris di DB.
func TestStore_DuplikatSerentakSatuBaris(t *testing.T) {
	svc := newService(t)
	p := Payment{PaymentID: "pay_9", TransactionID: "tx-1", Amount: 500}
	raw := []byte(`{"payment_id":"pay_9","transaction_id":"tx-1","amount":500}`)

	const requests = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	inserted, duplikat := 0, 0
	var gagal []error

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := svc.Store(context.Background(), p, raw)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				gagal = append(gagal, err)
			case ok:
				inserted++
			default:
				duplikat++
			}
		}()
	}
	wg.Wait()

	if len(gagal) > 0 {
		t.Fatalf("%d request error, contoh: %v", len(gagal), gagal[0])
	}
	if inserted != 1 || duplikat != requests-1 {
		t.Fatalf("harusnya 1 insert + %d duplikat, dapat %d + %d", requests-1, inserted, duplikat)
	}

	var n int
	if err := svc.DB.QueryRow(`SELECT COUNT(1) FROM transaction_payment`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("harusnya tepat 1 baris tersimpan, ada %d", n)
	}
}

func TestStore_PaymentBerbedaTetapMasuk(t *testing.T) {
	svc := newService(t)

	for _, id := range []string{"pay_1", "pay_2"} {
		ok, err := svc.Store(context.Background(),
			Payment{PaymentID: id, TransactionID: "tx-1", Amount: 100}, []byte(`{}`))
		if err != nil || !ok {
			t.Fatalf("payment %s harusnya tersimpan sebagai baris baru (ok=%v err=%v)", id, ok, err)
		}
	}

	var n int
	if err := svc.DB.QueryRow(`SELECT COUNT(1) FROM transaction_payment`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("harusnya 2 baris, ada %d", n)
	}
}

func TestStore_TanpaPaymentID(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Store(context.Background(), Payment{TransactionID: "tx-1"}, []byte(`{}`)); err == nil {
		t.Fatal("payment tanpa payment_id harusnya ditolak")
	}
}
