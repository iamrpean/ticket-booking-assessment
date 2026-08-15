# Ticket Booking - Backend Assessment

Service pemesanan tiket konser untuk technical assessment. Dibangun dengan Go + PostgreSQL, dijalankan lewat Docker Compose.

## Menjalankan

```
docker compose up --build
```

Compose menyalakan PostgreSQL lalu aplikasi (menunggu database sehat dulu). Aplikasi menjalankan dua listener:

- `:8080` - API utama
- `:9090` - mock accounting pihak ketiga, sengaja membalas `500` dua kali pertama per kiriman untuk mendemonstrasikan retry Scenario 3 (atur lewat `MOCK_FAIL_FIRST`)

Kalau port default bentrok di mesinmu, override lewat env host: `PORT=8096 MOCK_PORT=9096 DB_PORT=5434 docker compose up`.

Bisa juga jalan tanpa container untuk development: `docker compose up -d db` lalu `go run ./cmd/server` (koneksi diatur env `DATABASE_URL`, default menunjuk `localhost:5432`). Env lain: `ACCOUNTING_URL` (default menunjuk ke mock). Saat start, tiket `vip-1` di-seed dengan stok 1 sesuai skenario assessment.

## Scenario 1 - Race Condition

**Masalah.** Alur lama: baca stok -> cek di aplikasi -> kurangi stok -> simpan transaksi. Antara "baca" dan "kurangi" ada jeda waktu. Dua request yang datang hampir bersamaan sama-sama membaca stok `1`, sama-sama lolos pengecekan, dan sama-sama jadi membeli. Ujungnya 1 tiket terjual 2 kali.

**Solusi.** Pengecekan dan pengurangan stok dipindah ke database sebagai satu statement atomik, dibungkus satu transaksi bersama insert pembelian:

```sql
UPDATE tickets SET stock = stock - 1 WHERE id = ? AND stock > 0
-- rows affected = 1 -> INSERT purchases, COMMIT -> 201
-- rows affected = 0 -> tiket habis, ROLLBACK -> 409
```

Database mengeksekusi UPDATE pada baris yang sama secara serial, jadi hanya satu request yang menemukan `stock > 0` bernilai benar. Request yang kalah tidak "lolos cek lalu gagal tulis", karena cek dan tulisnya memang satu operasi.

**Asumsi.** Satu instance aplikasi cukup untuk skala assessment. Pola conditional update ini SQL standar; perilakunya sama di MySQL atau database relasional lain, jadi solusinya tidak terikat PostgreSQL.

**Trade-off.** Penulisan ke baris tiket yang sama otomatis terserialisasi. Aman, tapi jadi titik antrian kalau satu tiket diserbu ekstrem (puluhan ribu req/detik ke baris yang sama). Di skala itu alternatifnya reservation queue atau stok terpartisi, dengan kompleksitas jauh lebih tinggi.

```mermaid
flowchart TD
    A[POST /purchase] --> B[BEGIN transaksi]
    B --> C{"UPDATE tickets SET stock = stock - 1
    WHERE id = ? AND stock > 0"}
    C -->|rows affected = 1| D[INSERT purchases]
    D --> E[COMMIT]
    E --> F[201 Created]
    C -->|rows affected = 0| G[ROLLBACK]
    G --> H[409 Conflict - tiket habis]
```

**Demo.** Dua pembeli berebut 1 tiket VIP terakhir, satu dapat `201`, satunya `409`:

```
curl -s -X POST localhost:8080/purchase -d '{"ticket_id":"vip-1","user_id":"andi","amount":500}' &
curl -s -X POST localhost:8080/purchase -d '{"ticket_id":"vip-1","user_id":"budi","amount":500}' &
wait
curl -s localhost:8080/tickets/vip-1   # stok akhir: 0
```

## Scenario 2 - High Traffic Processing

**Masalah.** Lebih dari 10.000 transaksi masuk dalam waktu kurang dari 1 menit, dan setiap transaksi yang dijawab sukses harus benar-benar tersimpan, tidak boleh ada yang hilang diam-diam.

**Analisis.** Dua jebakan umum di sini. Pertama, commit database per request: tiap commit memaksa fsync, jadi throughput mentok jauh di bawah kebutuhan. Kedua, solusi naifnya "lempar ke antrian lalu langsung balas sukses": throughput naik, tapi janji sukses diberikan *sebelum* data aman. Di situlah transaksi hilang kalau proses mati.

**Solusi.** Antrian bounded + satu worker yang menulis secara batch, dengan aturan ketat: **response sukses baru dikirim setelah batch berisi transaksi itu ter-commit**.

- Handler memasukkan transaksi ke antrian lalu *menunggu* hasil commit-nya.
- Worker mengumpulkan sampai 200 transaksi atau 20ms (mana yang lebih dulu), menulis semuanya dalam satu transaksi DB, lalu membangunkan semua yang menunggu. Ratusan insert menumpang satu fsync.
- Antrian penuh -> `Submit` menunggu (backpressure), bukan menerima-lalu-hilang.
- Saat shutdown (SIGTERM), server berhenti menerima request baru lalu menguras sisa antrian sampai habis sebelum keluar.

Hasil di mesin uji (postgres dalam docker): 10.000 transaksi persisten dalam ~2,6 detik alias ~3.900 tx/detik. Kebutuhan soal hanya ~167 tx/detik.

**Asumsi.** "Sukses" didefinisikan dari sudut pandang client: transaksi disebut sukses hanya kalau sudah menerima `201`, dan `201` hanya keluar setelah data persisten. Client yang tidak menerima jawaban (timeout/putus) wajib menganggap statusnya tidak pasti dan boleh mengirim ulang.

**Trade-off.** Latency per request naik sebesar jendela flush (maksimal 20ms), harga yang wajar untuk jaminan durability. Satu baris rusak menggagalkan seluruh batch-nya (semua waiter diberi error, tidak ada yang dibohongi "sukses"). Antrian in-memory hilang kalau proses crash, tapi karena ack-setelah-persist, yang hilang hanyalah request yang memang belum dijawab sukses, jadi client tahu dan bisa retry. Di skala multi-node, antrian ini digantikan message broker durable (Kafka/RabbitMQ) dengan prinsip yang sama: ack setelah persisten.

```mermaid
flowchart TD
    A["POST /transactions (banyak client)"] --> B[Handler: enqueue + tunggu hasil]
    B --> Q[("Antrian bounded
    penuh -> backpressure")]
    Q --> W[Worker tunggal]
    W --> X{"batch 200 item
    atau lewat 20ms?"}
    X --> T["BEGIN -> INSERT xN -> COMMIT
    (satu fsync untuk satu batch)"]
    T -->|commit sukses| Y[Bangunkan waiter -> 201]
    T -->|commit gagal| Z[Bangunkan waiter -> 500]
```

**Demo.**

```
curl -s -X POST localhost:8080/transactions -d '{"ticket_id":"vip-1","user_id":"cici","amount":100}'
# {"id":"tx-...","status":"stored"}  (respon dikirim SETELAH data ter-commit)
```

## Scenario 3 - External API Integration

**Masalah.** Setiap transaksi sukses harus sampai ke accounting software pihak ketiga (`POST /transaction`), tapi pihak ketiga bisa membalas `HTTP 500`, timeout, atau tidak terjangkau. Dua cara yang salah: mengirim *sebelum* commit (kalau commit batal, accounting mencatat transaksi yang tidak pernah ada) dan mengirim *setelah* commit tanpa pencatatan (kalau proses mati di antara keduanya, transaksi tersimpan tapi tidak pernah terkirim, alias hilang diam-diam).

**Solusi: transactional outbox.** Niat mengirim dicatat sebagai baris di tabel `outbox` **dalam transaksi database yang sama** dengan penulisan transaksinya, jadi dua-duanya atomik: sama-sama tersimpan, atau sama-sama batal. Dispatcher terpisah lalu membaca baris `PENDING` dan mengirimkannya:

- Gagal (`500`/timeout/putus) -> dijadwalkan ulang dengan **exponential backoff** (1s, 2s, 4s, ... maks 30s) supaya pihak ketiga yang sedang bermasalah tidak makin terbebani.
- Setelah 8 kali gagal -> status `DEAD` (dead letter): berhenti retry otomatis, menunggu penanganan manual. Baris yang memang rusak tidak boleh di-retry tanpa akhir.
- Setiap kiriman membawa header `X-Idempotency-Key` yang stabil di semua retry, supaya pihak ketiga bisa mendeteksi kiriman ulang (retry sukses yang balasannya hilang di jalan tidak jadi dobel di sisi mereka).

Kedua jalur transaksi memakai pola ini: pembelian tiket (S1) dan ingest volume tinggi (S2, baris outbox ikut di transaksi batch).

**Asumsi.** Pihak ketiga pada akhirnya pulih (transient failure); jaminan yang diberikan *at-least-once delivery* + idempotency key, karena *exactly-once* lintas jaringan tidak mungkin tanpa kerja sama kedua sisi.

**Trade-off.** Ada jeda antara transaksi tersimpan dan terkirim ke accounting (eventual consistency). Ini konsekuensi yang memang diinginkan: pembelian user tidak boleh gagal cuma karena sistem accounting sedang down. Polling dispatcher menambah beban baca ringan; di skala besar polling diganti CDC/notifikasi, polanya tetap sama.

```mermaid
flowchart TD
    A["Transaksi sukses (purchase / ingest)"] --> B["Transaksi DB yang sama:
    INSERT data + INSERT outbox status PENDING"]
    B --> C[COMMIT atomik]
    C --> D["Dispatcher: poll baris PENDING
    yang sudah jatuh tempo"]
    D --> E["POST /transaction
    + X-Idempotency-Key"]
    E -->|2xx| F[status = SENT]
    E -->|500 / timeout| G{"attempts < 8?"}
    G -->|ya| H["backoff eksponensial
    next_retry = now + 1s*2^n (maks 30s)"]
    H --> D
    G -->|tidak| I["status = DEAD
    (dead letter, penanganan manual)"]
```

**Demo.** Mock accounting di `:9090` sengaja membalas `500` dua kali pertama per kiriman:

```
curl -s -X POST localhost:8080/purchase -d '{"ticket_id":"vip-1","user_id":"andi","amount":500}'
curl -s localhost:8080/outbox/stats     # {"PENDING":1}
# log server: percobaan 1 gagal (http 500), retry dalam 1s ... percobaan 2 ... terkirim
sleep 4 && curl -s localhost:8080/outbox/stats   # {"SENT":1}
curl -s localhost:9090/stats            # {"received":1} - diterima tepat sekali
```

## Scenario 4 - Duplicate Request

**Masalah.** Pihak ketiga memproses transaksi di background lalu mengirim webhook berisi data payment untuk disimpan ke `transaction_payment`. Karena respon kita tidak sampai ke mereka (masalah jaringan), mereka retry. Karena anomali, dua request dengan data identik bahkan bisa datang **di waktu yang sama**. Tanpa penanganan, satu payment tercatat dua kali.

**Analisis.** Ini race condition Scenario 1 dalam wujud lain. Dedup yang dicek di aplikasi ("SELECT dulu, kalau belum ada baru INSERT") punya celah waktu yang sama: dua request bersamaan sama-sama tidak menemukan baris, lalu sama-sama insert. Pengecekannya harus atomik.

**Solusi.** Keunikan ditegakkan di database lewat `UNIQUE(payment_id)` + `ON CONFLICT DO NOTHING`:

- Request pertama (siapa pun pemenangnya) -> baris tersimpan -> `200 {"status":"stored"}`.
- Request duplikat, meski datang bersamaan -> diserap constraint -> `200 {"duplicate":true}`.
- Duplikat **tetap dibalas 200**, bukan error. Konsumer idempoten yang membalas error untuk duplikat justru membuat pihak ketiga retry tanpa henti; 200 artinya "data ini sudah aman di saya", dan itu benar.

Ini pasangan simetris dari Scenario 3: saat mengirim, kita menyertakan `X-Idempotency-Key` supaya pihak ketiga bisa dedup; saat menerima, kita melakukan dedup yang sama memakai `payment_id` mereka.

**Asumsi.** `payment_id` dari pihak ketiga stabil antar retry (itulah kontrak idempotency key). Kalau pihak ketiga tidak menyediakannya, gantinya hash deterministik dari konten payload. Payload mentah ikut disimpan untuk audit.

**Trade-off.** Biayanya cuma satu unique index. Perlu disepakati juga masa simpan key-nya; di sini permanen (ukuran data assessment), di produksi biasanya cukup selama jendela retry pihak ketiga.

```mermaid
flowchart TD
    A[Webhook payment tiba] --> B["INSERT INTO transaction_payment
    ON CONFLICT(payment_id) DO NOTHING"]
    B -->|rows affected = 1| C["baris baru tersimpan
    200 stored"]
    B -->|rows affected = 0| D["duplikat diserap
    200 ok, duplicate:true"]
    C --> E[Pihak ketiga berhenti retry]
    D --> E
```

**Demo.** Kirim webhook identik dua kali (boleh paralel):

```
BODY='{"payment_id":"pay_9","transaction_id":"tx-1","amount":500}'
curl -s -X POST localhost:8080/webhook/payment -d "$BODY"   # {"status":"stored"}
curl -s -X POST localhost:8080/webhook/payment -d "$BODY"   # {"duplicate":true,"status":"ok"}
```

## Scenario 5 - Data Synchronization

**Masalah.** Sistem tiket mengirim update ketersediaan ke sistem lain: update pertama `quantity=5`, update kedua `quantity=2`. Karena latency jaringan, update kedua tiba lebih dulu, lalu update pertama yang telat menimpanya. Sistem tujuan menampilkan 5, padahal kondisi sebenarnya 2. Akar masalahnya: *last-write-wins berdasarkan waktu kedatangan*, padahal jaringan tidak pernah menjanjikan urutan kedatangan.

**Solusi.** Urutan tidak boleh disimpulkan dari kedatangan; urutan harus dibawa oleh datanya sendiri. Setiap update membawa `version` yang monoton naik dari sistem sumber, dan penerima hanya menerapkan update yang version-nya **lebih besar** dari yang tersimpan:

```sql
INSERT INTO ticket_availability (ticket_id, quantity, version) VALUES (?, ?, ?)
ON CONFLICT(ticket_id) DO UPDATE SET quantity = excluded.quantity, version = excluded.version
WHERE excluded.version > ticket_availability.version
```

Guard-nya berada **di dalam** statement upsert, bukan "SELECT version dulu, bandingkan di aplikasi, baru tulis" yang punya celah waktu yang sama dengan Scenario 1. Update basi dijawab `200 {"applied":false,"reason":"stale version"}`: tetap di-ack supaya pengirim tidak retry, tapi datanya diabaikan.

**Asumsi.** Sumber sanggup menerbitkan version monoton per tiket (counter yang di-increment atomik di database sumber). Alternatif version: timestamp sumber (rentan clock skew antar mesin) atau sequence dari message broker.

**Trade-off.** Update dikirim *full-state* (nilai quantity utuh), bukan delta. Kalau ada update yang hilang di tengah, update terbaru tetap self-contained dan hasil akhirnya benar; delta menuntut urutan sempurna tanpa lubang, jauh lebih rapuh. Harganya: semua pengirim harus disiplin lewat kontrak version, karena satu pengirim saja yang menulis tanpa version sudah merusak jaminannya.

```mermaid
flowchart TD
    A["Update tiba (ticket_id, quantity, version)"] --> B["Upsert dengan guard:
    WHERE excluded.version > tersimpan"]
    B -->|"version lebih baru
    rows affected = 1"| C["Data diperbarui
    200 applied:true"]
    B -->|"version lama / sama
    rows affected = 0"| D["Update basi diabaikan
    200 applied:false (stale)"]
```

**Demo.** Update v2 tiba duluan, v1 telat:

```
curl -s -X POST localhost:8080/sync/availability -d '{"ticket_id":"vip-1","quantity":2,"version":2}'   # applied:true
curl -s -X POST localhost:8080/sync/availability -d '{"ticket_id":"vip-1","quantity":5,"version":1}'   # applied:false, stale
curl -s localhost:8080/sync/availability/vip-1    # quantity=2, version=2
```

## Diagram flow gabungan

Kelima skenario dalam satu kesatuan: satu aplikasi, satu database, dua arah integrasi dengan dunia luar.

```mermaid
flowchart LR
    subgraph Luar["Dunia luar"]
        B[Pembeli]
        T[Client transaksi massal]
        A["Accounting pihak ketiga
        (bisa 500 / retry / duplikat)"]
        S[Sistem sumber availability]
    end

    subgraph App["Ticket booking service"]
        P["S1 POST /purchase
        cek+kurangi stok atomik"]
        I["S2 POST /transactions
        antrian -> batch commit -> ack"]
        D["S3 outbox dispatcher
        retry backoff + dead letter"]
        W["S4 POST /webhook/payment
        dedup unique constraint"]
        V["S5 POST /sync/availability
        version guard"]
    end

    DB[("PostgreSQL
    tickets, purchases, transactions,
    outbox, transaction_payment,
    ticket_availability")]

    B --> P
    T --> I
    P -->|"+ baris outbox (satu tx)"| DB
    I -->|"+ baris outbox (satu tx)"| DB
    DB -->|poll PENDING| D
    D -->|"POST /transaction
    X-Idempotency-Key"| A
    A -->|webhook payment| W
    W --> DB
    S -->|update quantity+version| V
    V --> DB
```

Benang merah desainnya dua hal. Pertama, **keputusan kritis dieksekusi sebagai satu operasi atomik di database**: cek-dan-kurangi stok (S1), simpan-dan-jadwalkan-kirim (S3), tolak-duplikat (S4), tolak-basi (S5). Alasannya, pengecekan di level aplikasi selalu menyisakan celah waktu antara baca dan tulis. Kedua, **jaringan dianggap selalu bisa gagal di titik mana pun**: sukses baru diakui setelah data persisten (S2), kiriman keluar dicatat dulu lalu di-retry sampai terkirim (S3), dan pesan masuk yang dobel atau telat dikenali lalu diserap tanpa merusak data (S4, S5).

## Menjalankan demo semua skenario

Dengan server berjalan, di terminal lain:

```
./scripts/demo.sh
```

Script menembakkan kelima skenario berurutan via curl (port custom: `BASE=localhost:8095 MOCK=localhost:9095 ./scripts/demo.sh`).

## Testing

Test jalan melawan postgres sungguhan (bukan mock), jadi nyalakan database-nya dulu:

```
docker compose up -d db
go test -race ./...
```

Tiap test membuat schema sekali pakai dan membuangnya saat selesai, jadi test antar package aman jalan paralel. Postgres di lokasi lain bisa dioverride lewat `TEST_DATABASE_URL`.

Test kunci per skenario:

- `TestBuy_SatuTiketBanyakPembeli` (S1) - 100 pembeli serentak ke 1 tiket tersisa: tepat satu berhasil, sisanya `409`, stok akhir 0, hanya 1 baris purchase.
- `TestSubmit_10RibuSemuaTersimpan` (S2) - 10.000 submit serentak: semua dijawab sukses dan semuanya terhitung di database.
- `TestSubmit_BatchGagalSemuaDapatError` (S2) - kalau satu batch gagal commit, semua request di batch itu dapat error, tidak ada yang dijawab sukses palsu.
- `TestDispatcher_Retry500SampaiTerkirim` (S3) - pihak ketiga membalas 500 dua kali lalu pulih: retry jalan, berakhir `SENT`, diterima tepat sekali.
- `TestDispatcher_DeadLetterSetelahMaxAttempts` (S3) - pihak ketiga mati total: berhenti di `DEAD` setelah batas percobaan, tidak retry selamanya.
- `TestBuy_MencatatOutbox` / `TestSubmit_MencatatOutbox` (S3) - baris outbox lahir di transaksi DB yang sama; pembelian gagal tidak meninggalkan outbox.
- `TestStore_DuplikatSerentakSatuBaris` (S4) - 50 webhook identik serentak: semua dijawab sukses, tepat 1 baris tersimpan.
- `TestApply_UpdateTerbalik` (S5) - v2 tiba duluan, v1 telat: yang basi ditolak, hasil akhir qty=2.
- `TestApply_KonkurenVersiTertinggiMenang` (S5) - 30 versi datang serentak dalam urutan acak: version tertinggi selalu menang.
