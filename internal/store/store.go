package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const schema = `
CREATE TABLE IF NOT EXISTS tickets (
	id    TEXT PRIMARY KEY,
	name  TEXT NOT NULL,
	stock INTEGER NOT NULL CHECK (stock >= 0)
);

CREATE TABLE IF NOT EXISTS purchases (
	id         BIGSERIAL PRIMARY KEY,
	ticket_id  TEXT NOT NULL REFERENCES tickets(id),
	user_id    TEXT NOT NULL,
	amount     BIGINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
	id         TEXT PRIMARY KEY,
	ticket_id  TEXT NOT NULL,
	user_id    TEXT NOT NULL,
	amount     BIGINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox: baris dibuat dalam transaksi DB yang SAMA dengan penulisan
-- transaksinya, lalu dikirim ke pihak ketiga oleh dispatcher terpisah.
-- next_retry_at berupa unix milli supaya perhitungan backoff ada di Go.
CREATE TABLE IF NOT EXISTS outbox (
	id            BIGSERIAL PRIMARY KEY,
	kind          TEXT NOT NULL,             -- 'purchase' | 'transaction'
	ref_id        TEXT NOT NULL,
	payload       TEXT NOT NULL,
	status        TEXT NOT NULL DEFAULT 'PENDING', -- PENDING | SENT | DEAD
	attempts      INTEGER NOT NULL DEFAULT 0,
	next_retry_at BIGINT NOT NULL DEFAULT 0,
	last_error    TEXT,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
	sent_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_retry_at);

-- Payment dari webhook pihak ketiga. UNIQUE(payment_id) adalah kunci
-- idempotensi: request duplikat tidak akan pernah jadi dua baris,
-- seberapa pun bersamaan datangnya.
CREATE TABLE IF NOT EXISTS transaction_payment (
	id             BIGSERIAL PRIMARY KEY,
	payment_id     TEXT NOT NULL UNIQUE,
	transaction_id TEXT NOT NULL,
	amount         BIGINT NOT NULL,
	payload        TEXT NOT NULL,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ketersediaan tiket versi sistem tujuan (sisi penerima sinkronisasi).
-- version monoton naik dipakai menolak update yang datang terlambat.
CREATE TABLE IF NOT EXISTS ticket_availability (
	ticket_id  TEXT PRIMARY KEY,
	quantity   BIGINT NOT NULL,
	version    BIGINT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Open membuka koneksi postgres dan menjalankan migrasi skema. Ping
// di-retry sebentar supaya app tidak mati cuma karena database masih
// dalam proses start (urutan container).
func Open(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	// Postgres membatasi jumlah koneksi (default 100). Pool dibatasi supaya
	// lonjakan goroutine mengantre di pool, bukan menjebol max_connections.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			db.Close()
			return nil, fmt.Errorf("database tidak bisa dihubungi: %w", err)
		case <-time.After(500 * time.Millisecond):
		}
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrasi skema: %w", err)
	}
	return db, nil
}

// SeedTicket membuat tiket kalau belum ada, dipakai untuk demo & test.
func SeedTicket(db *sql.DB, id, name string, stock int) error {
	_, err := db.Exec(`INSERT INTO tickets (id, name, stock) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING`, id, name, stock)
	return err
}
