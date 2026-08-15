package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS tickets (
	id    TEXT PRIMARY KEY,
	name  TEXT NOT NULL,
	stock INTEGER NOT NULL CHECK (stock >= 0)
);

CREATE TABLE IF NOT EXISTS purchases (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	ticket_id  TEXT NOT NULL REFERENCES tickets(id),
	user_id    TEXT NOT NULL,
	amount     INTEGER NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
	id         TEXT PRIMARY KEY,
	ticket_id  TEXT NOT NULL,
	user_id    TEXT NOT NULL,
	amount     INTEGER NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Outbox: baris dibuat dalam transaksi DB yang SAMA dengan penulisan
-- transaksinya, lalu dikirim ke pihak ketiga oleh dispatcher terpisah.
-- next_retry_at berupa unix milli supaya perhitungan backoff ada di Go.
CREATE TABLE IF NOT EXISTS outbox (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	kind          TEXT NOT NULL,             -- 'purchase' | 'transaction'
	ref_id        TEXT NOT NULL,
	payload       TEXT NOT NULL,
	status        TEXT NOT NULL DEFAULT 'PENDING', -- PENDING | SENT | DEAD
	attempts      INTEGER NOT NULL DEFAULT 0,
	next_retry_at INTEGER NOT NULL DEFAULT 0,
	last_error    TEXT,
	created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	sent_at       DATETIME
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_retry_at);

-- Payment dari webhook pihak ketiga. UNIQUE(payment_id) adalah kunci
-- idempotensi: request duplikat tidak akan pernah jadi dua baris,
-- seberapa pun bersamaan datangnya.
CREATE TABLE IF NOT EXISTS transaction_payment (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	payment_id     TEXT NOT NULL UNIQUE,
	transaction_id TEXT NOT NULL,
	amount         INTEGER NOT NULL,
	payload        TEXT NOT NULL,
	created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Ketersediaan tiket versi sistem tujuan (sisi penerima sinkronisasi).
-- version monoton naik dipakai menolak update yang datang terlambat.
CREATE TABLE IF NOT EXISTS ticket_availability (
	ticket_id  TEXT PRIMARY KEY,
	quantity   INTEGER NOT NULL,
	version    INTEGER NOT NULL,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

// Open membuka database sqlite dan menjalankan migrasi skema.
func Open(path string) (*sql.DB, error) {
	// synchronous=NORMAL aman dikombinasikan dengan WAL dan memangkas fsync
	// per commit - penting saat volume tulis tinggi.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrasi skema: %w", err)
	}
	return db, nil
}

// SeedTicket membuat tiket kalau belum ada, dipakai untuk demo & test.
func SeedTicket(db *sql.DB, id, name string, stock int) error {
	_, err := db.Exec(`INSERT INTO tickets (id, name, stock) VALUES (?, ?, ?)
		ON CONFLICT(id) DO NOTHING`, id, name, stock)
	return err
}
