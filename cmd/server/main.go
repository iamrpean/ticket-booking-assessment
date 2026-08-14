package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/iamrpean/ticket-booking-assessment/internal/booking"
	"github.com/iamrpean/ticket-booking-assessment/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "data.db"
	}

	db, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("buka db: %v", err)
	}
	defer db.Close()

	// skenario assessment: tersisa 1 tiket VIP
	if err := store.SeedTicket(db, "vip-1", "VIP Konser", 1); err != nil {
		log.Fatalf("seed tiket: %v", err)
	}

	bookingSvc := &booking.Service{DB: db}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /purchase", handlePurchase(bookingSvc))
	mux.HandleFunc("GET /tickets/{id}", handleGetTicket(db))

	log.Printf("server listen di :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

type purchaseReq struct {
	TicketID string `json:"ticket_id"`
	UserID   string `json:"user_id"`
	Amount   int64  `json:"amount"`
}

func handlePurchase(svc *booking.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req purchaseReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body bukan json valid"})
			return
		}
		if req.TicketID == "" || req.UserID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ticket_id dan user_id wajib diisi"})
			return
		}

		p, err := svc.Buy(r.Context(), req.TicketID, req.UserID, req.Amount)
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
}

func handleGetTicket(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var name string
		var stock int
		err := db.QueryRow(`SELECT name, stock FROM tickets WHERE id = ?`, id).Scan(&name, &stock)
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
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
