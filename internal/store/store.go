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
