package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iamrpean/ticket-booking-assessment/internal/api"
	"github.com/iamrpean/ticket-booking-assessment/internal/booking"
	"github.com/iamrpean/ticket-booking-assessment/internal/ingest"
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
	ingestSvc := ingest.New(db, 4096, 200, 20*time.Millisecond)

	srv := &http.Server{
		Addr: ":" + port,
		Handler: api.New(api.Deps{
			DB:      db,
			Booking: bookingSvc,
			Ingest:  ingestSvc,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("server listen di :%s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown: menyelesaikan request aktif lalu menguras antrian ingest")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	ingestSvc.Close() // setelah server berhenti terima request, kuras sisa antrian
}
