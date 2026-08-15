package syncsvc

import (
	"context"
	"database/sql"
	"errors"
)

var ErrInvalid = errors.New("ticket_id wajib diisi dan version harus > 0")

type Service struct {
	DB *sql.DB
}

type Update struct {
	TicketID string `json:"ticket_id"`
	Quantity int64  `json:"quantity"`
	Version  int64  `json:"version"`
}

// Apply menerapkan update ketersediaan hanya kalau version-nya lebih baru
// dari yang tersimpan. Urutan kedatangan jadi tidak penting: update lama
// yang tiba belakangan (karena latency) kalah oleh guard version di dalam
// satu statement atomik - bukan dibandingkan dulu di aplikasi lalu ditulis,
// celah yang sama seperti Scenario 1.
//
// Return: applied=false artinya update basi dan diabaikan.
func (s *Service) Apply(ctx context.Context, u Update) (bool, error) {
	if u.TicketID == "" || u.Version <= 0 {
		return false, ErrInvalid
	}
	res, err := s.DB.ExecContext(ctx, `
		INSERT INTO ticket_availability (ticket_id, quantity, version)
		VALUES ($1, $2, $3)
		ON CONFLICT (ticket_id) DO UPDATE SET
			quantity   = excluded.quantity,
			version    = excluded.version,
			updated_at = now()
		WHERE excluded.version > ticket_availability.version`,
		u.TicketID, u.Quantity, u.Version)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Get mengembalikan ketersediaan yang tersimpan untuk satu tiket.
func (s *Service) Get(ctx context.Context, ticketID string) (Update, error) {
	u := Update{TicketID: ticketID}
	err := s.DB.QueryRowContext(ctx,
		`SELECT quantity, version FROM ticket_availability WHERE ticket_id = $1`,
		ticketID).Scan(&u.Quantity, &u.Version)
	return u, err
}
