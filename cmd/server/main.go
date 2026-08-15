package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/iamrpean/ticket-booking-assessment/internal/api"
	"github.com/iamrpean/ticket-booking-assessment/internal/booking"
	"github.com/iamrpean/ticket-booking-assessment/internal/ingest"
	"github.com/iamrpean/ticket-booking-assessment/internal/mockacct"
	"github.com/iamrpean/ticket-booking-assessment/internal/outbox"
	"github.com/iamrpean/ticket-booking-assessment/internal/store"
	"github.com/iamrpean/ticket-booking-assessment/internal/webhook"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := env("PORT", "8080")
	dbPath := env("DB_PATH", "data.db")
	mockPort := env("MOCK_PORT", "9090")
	accountingURL := env("ACCOUNTING_URL", "http://localhost:"+mockPort+"/transaction")
	mockFailFirst, err := strconv.Atoi(env("MOCK_FAIL_FIRST", "2"))
	if err != nil {
		log.Fatalf("MOCK_FAIL_FIRST bukan angka: %v", err)
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
	webhookSvc := &webhook.Service{DB: db}
	dispatcher := &outbox.Dispatcher{
		DB:          db,
		TargetURL:   accountingURL,
		Client:      &http.Client{Timeout: 5 * time.Second},
		PollEvery:   500 * time.Millisecond,
		BaseBackoff: 1 * time.Second,
		MaxBackoff:  30 * time.Second,
		MaxAttempts: 8,
	}

	srv := &http.Server{
		Addr: ":" + port,
		Handler: api.New(api.Deps{
			DB:      db,
			Booking: bookingSvc,
			Ingest:  ingestSvc,
			Outbox:  dispatcher,
			Webhook: webhookSvc,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	mockSrv := &http.Server{
		Addr:              ":" + mockPort,
		Handler:           mockacct.New(mockFailFirst).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go dispatcher.Run(ctx)
	go func() {
		log.Printf("mock accounting di :%s (membalas 500 %dx pertama per kiriman)", mockPort, mockFailFirst)
		if err := mockSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
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
	_ = mockSrv.Shutdown(shutdownCtx)
	ingestSvc.Close() // setelah server berhenti terima request, kuras sisa antrian
}
