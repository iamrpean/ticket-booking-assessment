package booking

import (
	"context"
	"database/sql"
	"errors"
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
		`UPDATE tickets SET stock = stock - 1 WHERE id = ? AND stock > 0`, ticketID)
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
			`SELECT COUNT(1) FROM tickets WHERE id = ?`, ticketID).Scan(&exists); err != nil {
			return nil, err
		}
		if exists == 0 {
			return nil, ErrNotFound
		}
		return nil, ErrSoldOut
	}

	res, err = tx.ExecContext(ctx,
		`INSERT INTO purchases (ticket_id, user_id, amount) VALUES (?, ?, ?)`,
		ticketID, userID, amount)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Purchase{ID: id, TicketID: ticketID, UserID: userID, Amount: amount}, nil
}
