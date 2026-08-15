package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Dispatcher mengambil baris outbox berstatus PENDING yang sudah jatuh
// tempo lalu mengirimkannya ke pihak ketiga. Gagal (HTTP non-2xx, timeout,
// jaringan putus) -> dijadwalkan ulang dengan exponential backoff. Setelah
// MaxAttempts kali gagal -> status DEAD (dead letter, perlu penanganan
// manual) supaya baris rusak tidak di-retry selamanya.
//
// Pengiriman menyertakan header X-Idempotency-Key (id baris outbox) yang
// stabil di semua retry, supaya pihak ketiga bisa mendeteksi kiriman ulang.
type Dispatcher struct {
	DB          *sql.DB
	TargetURL   string
	Client      *http.Client
	PollEvery   time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	MaxAttempts int
}

// Run memproses outbox sampai ctx dibatalkan.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.PollEvery)
	defer ticker.Stop()
	for {
		d.processDue(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) processDue(ctx context.Context) {
	rows, err := d.DB.QueryContext(ctx, `
		SELECT id, payload, attempts FROM outbox
		WHERE status = 'PENDING' AND next_retry_at <= ?
		ORDER BY id LIMIT 50`, time.Now().UnixMilli())
	if err != nil {
		log.Printf("outbox: query due: %v", err)
		return
	}
	type row struct {
		id       int64
		payload  string
		attempts int
	}
	var due []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.payload, &r.attempts); err != nil {
			log.Printf("outbox: scan: %v", err)
			break
		}
		due = append(due, r)
	}
	rows.Close()

	for _, r := range due {
		if ctx.Err() != nil {
			return
		}
		if err := d.send(ctx, r.id, r.payload); err != nil {
			d.recordFailure(r.id, r.attempts+1, err)
		} else {
			d.recordSent(r.id)
		}
	}
}

func (d *Dispatcher) send(ctx context.Context, id int64, payload string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetURL,
		bytes.NewBufferString(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", fmt.Sprintf("outbox-%d", id))

	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func (d *Dispatcher) recordSent(id int64) {
	if _, err := d.DB.Exec(
		`UPDATE outbox SET status = 'SENT', sent_at = datetime('now') WHERE id = ?`, id); err != nil {
		log.Printf("outbox: tandai sent #%d: %v", id, err)
		return
	}
	log.Printf("outbox: #%d terkirim", id)
}

func (d *Dispatcher) recordFailure(id int64, attempts int, cause error) {
	if attempts >= d.MaxAttempts {
		if _, err := d.DB.Exec(
			`UPDATE outbox SET status = 'DEAD', attempts = ?, last_error = ? WHERE id = ?`,
			attempts, cause.Error(), id); err != nil {
			log.Printf("outbox: tandai dead #%d: %v", id, err)
			return
		}
		log.Printf("outbox: #%d DEAD setelah %d percobaan (%v)", id, attempts, cause)
		return
	}

	backoff := d.BaseBackoff << (attempts - 1) // 1x, 2x, 4x, 8x, ...
	if backoff > d.MaxBackoff {
		backoff = d.MaxBackoff
	}
	if _, err := d.DB.Exec(
		`UPDATE outbox SET attempts = ?, next_retry_at = ?, last_error = ? WHERE id = ?`,
		attempts, time.Now().Add(backoff).UnixMilli(), cause.Error(), id); err != nil {
		log.Printf("outbox: jadwalkan retry #%d: %v", id, err)
		return
	}
	log.Printf("outbox: #%d percobaan %d gagal (%v), retry dalam %v", id, attempts, cause, backoff)
}

// Stats mengembalikan jumlah baris outbox per status.
func (d *Dispatcher) Stats(ctx context.Context) (map[string]int, error) {
	rows, err := d.DB.QueryContext(ctx, `SELECT status, COUNT(1) FROM outbox GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}
