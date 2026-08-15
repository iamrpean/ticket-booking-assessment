package booking

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

var (
	ErrSoldOut  = errors.New("tiket habis")
	ErrNotFound = errors.New("tiket tidak ditemukan")
)

type Service struct {
	DB *sql.DB
}

type Purchase struct {
	ID       int64  `json:"id"`
	TicketID string `json:"ticket_id"`
	UserID   string `json:"user_id"`
	Amount   int64  `json:"amount"`
}

// Buy mengurangi stok dan mencatat pembelian dalam satu transaksi DB.
// Pengecekan stok dan pengurangannya sengaja digabung ke satu UPDATE
// bersyarat supaya atomik di database - kalau dicek dulu di aplikasi
// (read lalu write), dua request bersamaan bisa sama-sama lolos.
func (s *Service) Buy(ctx context.Context, ticketID, userID string, amount int64) (*Purchase, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`UPDATE tickets SET stock = stock - 1 WHERE id = $1 AND stock > 0`, ticketID)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected == 0 {
		// kalah rebutan stok, atau tiketnya memang tidak ada
		var exists int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(1) FROM tickets WHERE id = $1`, ticketID).Scan(&exists); err != nil {
			return nil, err
		}
		if exists == 0 {
			return nil, ErrNotFound
		}
		return nil, ErrSoldOut
	}

	var id int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO purchases (ticket_id, user_id, amount) VALUES ($1, $2, $3) RETURNING id`,
		ticketID, userID, amount).Scan(&id); err != nil {
		return nil, err
	}

	// Baris outbox ditulis dalam transaksi yang SAMA (transactional outbox):
	// tidak mungkin ada pembelian sukses tanpa jadwal kirim ke accounting,
	// dan tidak mungkin ada jadwal kirim untuk pembelian yang batal.
	payload, _ := json.Marshal(map[string]any{
		"type":        "purchase",
		"purchase_id": id,
		"ticket_id":   ticketID,
		"user_id":     userID,
		"amount":      amount,
		"occurred_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (kind, ref_id, payload) VALUES ('purchase', $1, $2)`,
		strconv.FormatInt(id, 10), string(payload)); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Purchase{ID: id, TicketID: ticketID, UserID: userID, Amount: amount}, nil
}
