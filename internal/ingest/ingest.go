package ingest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
)

type Transaction struct {
	ID       string `json:"id"`
	TicketID string `json:"ticket_id"`
	UserID   string `json:"user_id"`
	Amount   int64  `json:"amount"`
}

type item struct {
	tx   Transaction
	done chan error
}

// Service menerima transaksi bervolume tinggi lewat antrian bounded dan
// menyimpannya ke database secara batch. Commit per-insert membuat SQLite
// fsync tiap request; dengan batch, ratusan insert menumpang satu commit.
//
// Submit baru kembali SETELAH batch berisi transaksinya ter-commit - jadi
// response sukses selalu berarti data sudah persisten, bukan "sudah masuk
// antrian". Kalau antrian penuh, Submit menunggu (backpressure), bukan
// menerima lalu diam-diam kehilangan data.
type Service struct {
	db         *sql.DB
	queue      chan item
	batchSize  int
	flushEvery time.Duration
	wg         sync.WaitGroup
}

func New(db *sql.DB, queueSize, batchSize int, flushEvery time.Duration) *Service {
	s := &Service{
		db:         db,
		queue:      make(chan item, queueSize),
		batchSize:  batchSize,
		flushEvery: flushEvery,
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

// Submit memasukkan transaksi ke antrian lalu menunggu hasil commit-nya.
// ID dibuatkan kalau kosong; ID yang dipakai dikembalikan ke caller.
func (s *Service) Submit(ctx context.Context, tx Transaction) (string, error) {
	if tx.ID == "" {
		tx.ID = newID()
	}
	it := item{tx: tx, done: make(chan error, 1)}

	select {
	case s.queue <- it:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	select {
	case err := <-it.done:
		return tx.ID, err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close menghentikan penerimaan dan menguras sisa antrian sampai habis.
// Panggil setelah HTTP server berhenti menerima request.
func (s *Service) Close() {
	close(s.queue)
	s.wg.Wait()
}

func (s *Service) loop() {
	defer s.wg.Done()

	batch := make([]item, 0, s.batchSize)
	timer := time.NewTimer(s.flushEvery)
	timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := s.insertBatch(batch)
		for i := range batch {
			batch[i].done <- err
		}
		batch = batch[:0]
		timer.Stop()
	}

	for {
		select {
		case it, ok := <-s.queue:
			if !ok {
				flush()
				return
			}
			if len(batch) == 0 {
				timer.Reset(s.flushEvery)
			}
			batch = append(batch, it)
			if len(batch) >= s.batchSize {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

func (s *Service) insertBatch(batch []item) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT INTO transactions (id, ticket_id, user_id, amount) VALUES ($1, $2, $3, $4)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// outbox ikut ditulis di transaksi batch yang sama (Scenario 3):
	// transaksi tersimpan dan jadwal kirim ke accounting-nya atomik.
	obStmt, err := tx.Prepare(
		`INSERT INTO outbox (kind, ref_id, payload) VALUES ('transaction', $1, $2)`)
	if err != nil {
		return err
	}
	defer obStmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for _, it := range batch {
		if _, err := stmt.Exec(it.tx.ID, it.tx.TicketID, it.tx.UserID, it.tx.Amount); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{
			"type":           "transaction",
			"transaction_id": it.tx.ID,
			"ticket_id":      it.tx.TicketID,
			"user_id":        it.tx.UserID,
			"amount":         it.tx.Amount,
			"occurred_at":    now,
		})
		if _, err := obStmt.Exec(it.tx.ID, string(payload)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "tx-" + hex.EncodeToString(b)
}
