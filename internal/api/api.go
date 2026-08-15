package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/iamrpean/ticket-booking-assessment/internal/booking"
	"github.com/iamrpean/ticket-booking-assessment/internal/ingest"
	"github.com/iamrpean/ticket-booking-assessment/internal/outbox"
	"github.com/iamrpean/ticket-booking-assessment/internal/webhook"
)

type Deps struct {
	DB      *sql.DB
	Booking *booking.Service
	Ingest  *ingest.Service
	Outbox  *outbox.Dispatcher
	Webhook *webhook.Service
}

func New(d Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /purchase", d.handlePurchase)
	mux.HandleFunc("GET /tickets/{id}", d.handleGetTicket)
	mux.HandleFunc("POST /transactions", d.handleSubmitTransaction)
	mux.HandleFunc("GET /outbox/stats", d.handleOutboxStats)
	mux.HandleFunc("POST /webhook/payment", d.handlePaymentWebhook)
	return mux
}

func (d Deps) handlePaymentWebhook(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "gagal membaca body"})
		return
	}
	var p webhook.Payment
	if err := json.Unmarshal(raw, &p); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body bukan json valid"})
		return
	}

	inserted, err := d.Webhook.Store(r.Context(), p, raw)
	switch {
	case errors.Is(err, webhook.ErrPaymentIDKosong):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payment_id wajib diisi"})
	case err != nil:
		log.Printf("webhook payment: %v", err)
		// 500 -> pihak ketiga akan retry; aman karena penyimpanan idempoten
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	case inserted:
		writeJSON(w, http.StatusOK, map[string]any{"status": "stored"})
	default:
		// duplikat tetap 200 - kalau dibalas error, pihak ketiga retry terus
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "duplicate": true})
	}
}

func (d Deps) handleOutboxStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.Outbox.Stats(r.Context())
	if err != nil {
		log.Printf("outbox stats: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

type purchaseReq struct {
	TicketID string `json:"ticket_id"`
	UserID   string `json:"user_id"`
	Amount   int64  `json:"amount"`
}

func (d Deps) handlePurchase(w http.ResponseWriter, r *http.Request) {
	var req purchaseReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body bukan json valid"})
		return
	}
	if req.TicketID == "" || req.UserID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ticket_id dan user_id wajib diisi"})
		return
	}

	p, err := d.Booking.Buy(r.Context(), req.TicketID, req.UserID, req.Amount)
	switch {
	case errors.Is(err, booking.ErrSoldOut):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tiket habis"})
	case errors.Is(err, booking.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tiket tidak ditemukan"})
	case err != nil:
		log.Printf("purchase: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	default:
		writeJSON(w, http.StatusCreated, p)
	}
}

func (d Deps) handleGetTicket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var name string
	var stock int
	err := d.DB.QueryRow(`SELECT name, stock FROM tickets WHERE id = ?`, id).Scan(&name, &stock)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tiket tidak ditemukan"})
		return
	}
	if err != nil {
		log.Printf("get ticket: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "name": name, "stock": stock})
}

func (d Deps) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	var tx ingest.Transaction
	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body bukan json valid"})
		return
	}
	if tx.TicketID == "" || tx.UserID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ticket_id dan user_id wajib diisi"})
		return
	}

	id, err := d.Ingest.Submit(r.Context(), tx)
	if err != nil {
		log.Printf("submit transaction: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "transaksi gagal disimpan"})
		return
	}
	// 201 hanya setelah commit - bukan setelah masuk antrian
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "stored"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
