package webhook

import (
	"context"
	"database/sql"
	"errors"
)

var ErrPaymentIDKosong = errors.New("payment_id wajib diisi")

type Service struct {
	DB *sql.DB
}

type Payment struct {
	PaymentID     string `json:"payment_id"`
	TransactionID string `json:"transaction_id"`
	Amount        int64  `json:"amount"`
}

// Store menyimpan payment secara idempoten. Deduplikasi TIDAK dilakukan
// dengan "SELECT dulu, kalau belum ada baru INSERT" - itu pola read-check-
// write yang sama rapuhnya dengan Scenario 1: dua request bersamaan sama-
// sama tidak menemukan baris, lalu sama-sama insert. Keunikan ditegakkan
// oleh UNIQUE(payment_id) di database; yang datang belakangan diserap
// ON CONFLICT DO NOTHING.
//
// Return: inserted=true kalau baris baru tersimpan, false kalau duplikat.
// Dua-duanya dianggap sukses oleh caller - konsumer idempoten harus
// membalas 200 untuk duplikat, kalau tidak pihak ketiga retry selamanya.
func (s *Service) Store(ctx context.Context, p Payment, raw []byte) (bool, error) {
	if p.PaymentID == "" {
		return false, ErrPaymentIDKosong
	}
	res, err := s.DB.ExecContext(ctx, `
		INSERT INTO transaction_payment (payment_id, transaction_id, amount, payload)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(payment_id) DO NOTHING`,
		p.PaymentID, p.TransactionID, p.Amount, string(raw))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
