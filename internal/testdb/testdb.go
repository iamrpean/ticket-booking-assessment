// Package testdb menyediakan koneksi postgres untuk test. Tiap test
// mendapat schema sekali pakai (search_path sendiri) supaya test antar
// package bisa jalan paralel tanpa saling mengotori data.
package testdb

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/iamrpean/ticket-booking-assessment/internal/store"
)

func url() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable"
}

// New membuka pool yang terkunci ke schema baru, sudah termigrasi.
func New(t *testing.T) *sql.DB {
	t.Helper()

	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	schema := "test_" + hex.EncodeToString(b)

	admin, err := sql.Open("pgx", url())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatalf("buat schema %s: %v (postgres jalan? coba: docker compose up -d db)", schema, err)
	}

	sep := "?"
	if strings.Contains(url(), "?") {
		sep = "&"
	}
	db, err := store.Open(url() + sep + "search_path=" + schema)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	return db
}
