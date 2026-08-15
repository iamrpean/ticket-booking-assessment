#!/usr/bin/env bash
# Demo end-to-end kelima skenario. Jalankan server dulu di terminal lain:
#   go run ./cmd/server
set -euo pipefail

BASE="${BASE:-localhost:8080}"
MOCK="${MOCK:-localhost:9090}"

judul() { echo; echo "=== $1 ==="; }

judul "Scenario 1 - Race Condition: 2 pembeli rebutan 1 tiket VIP"
curl -s -X POST "$BASE/purchase" -d '{"ticket_id":"vip-1","user_id":"andi","amount":500}' &
curl -s -X POST "$BASE/purchase" -d '{"ticket_id":"vip-1","user_id":"budi","amount":500}' &
wait
echo
echo "stok akhir:"
curl -s "$BASE/tickets/vip-1"
echo

judul "Scenario 2 - High Traffic: 50 transaksi paralel (bukti 10.000 ada di go test)"
for i in $(seq 1 50); do
  curl -s -o /dev/null -X POST "$BASE/transactions" \
    -d "{\"ticket_id\":\"vip-1\",\"user_id\":\"user-$i\",\"amount\":100}" &
done
wait
echo "50 request selesai, semua dijawab setelah commit"

judul "Scenario 3 - External API: outbox retry ke mock accounting (500 2x dulu)"
echo "stats outbox sekarang:"
curl -s "$BASE/outbox/stats"
echo
echo "menunggu retry backoff (~8 detik)..."
sleep 8
echo "stats outbox setelah retry:"
curl -s "$BASE/outbox/stats"
echo
echo "yang diterima mock accounting:"
curl -s "$MOCK/stats"
echo

judul "Scenario 4 - Duplicate Request: webhook payment identik 2x"
BODY='{"payment_id":"pay_9","transaction_id":"tx-1","amount":500}'
curl -s -X POST "$BASE/webhook/payment" -d "$BODY"
echo
curl -s -X POST "$BASE/webhook/payment" -d "$BODY"
echo

judul "Scenario 5 - Data Sync: update v2 tiba duluan, v1 telat harus diabaikan"
curl -s -X POST "$BASE/sync/availability" -d '{"ticket_id":"vip-1","quantity":2,"version":2}'
echo
curl -s -X POST "$BASE/sync/availability" -d '{"ticket_id":"vip-1","quantity":5,"version":1}'
echo
echo "hasil akhir (harus quantity=2, version=2):"
curl -s "$BASE/sync/availability/vip-1"
echo

judul "Selesai"
