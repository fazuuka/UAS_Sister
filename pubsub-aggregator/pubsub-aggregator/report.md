# Laporan UAS — Pub-Sub Log Aggregator Terdistribusi

**Tema:** Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi
**Mata Kuliah:** Sistem Pararel dan Terdistribusi

---

## Bagian I — Teori (Bab 1–13)

### T1 (Bab 1) — Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sebuah sistem terdistribusi didefinisikan sebagai kumpulan komponen independen yang berkomunikasi dan mengoordinasikan tindakannya hanya melalui pengiriman pesan, sehingga dari sudut pandang pengguna sistem tersebut tampak sebagai satu kesatuan yang koheren (Coulouris, Dollimore, Kindberg, & Blair, 2012). Karakteristik utama yang relevan dengan rancangan Pub-Sub Log Aggregator pada proyek ini meliputi: (1) **concurrency**, di mana banyak publisher dan consumer worker berjalan bersamaan tanpa koordinasi terpusat yang ketat; (2) **tidak ada clock global**, sehingga urutan event antar service tidak bisa diasumsikan sinkron; (3) **kegagalan independen**, di mana setiap service (aggregator, broker, storage) dapat gagal secara terpisah tanpa mematikan seluruh sistem.

Trade-off desain utama pada aggregator ini adalah antara **konsistensi** dan **ketersediaan/skalabilitas**. Dengan memilih model at-least-once delivery (event boleh terkirim berkali-kali) dibanding exactly-once yang sulit dijamin di sistem terdistribusi nyata, sistem mengorbankan kesederhanaan pengiriman demi ketersediaan, namun menebusnya dengan idempotent consumer di sisi penerima. Trade-off kedua adalah antara **latency** dan **durability**: setiap event diverifikasi lewat transaksi database sebelum dianggap "selesai", yang menambah latency dibanding pendekatan in-memory murni, tetapi menjamin data tidak hilang saat container di-restart.

### T2 (Bab 2) — Kapan Memilih Arsitektur Publish–Subscribe Dibanding Client–Server?

Arsitektur client–server bersifat synchronous dan tightly-coupled: klien mengirim permintaan dan menunggu balasan langsung dari server yang dikenal alamatnya. Publish–subscribe, sebaliknya, adalah salah satu bentuk **indirect communication** di mana pengirim (publisher) dan penerima (subscriber) tidak perlu mengetahui identitas satu sama lain; komunikasi dimediasi oleh sebuah perantara yang menangani penyaluran event berdasarkan topic (Coulouris et al., 2012).

Publish–subscribe dipilih untuk log aggregator ini karena tiga alasan teknis. Pertama, **decoupling temporal**: publisher (penghasil log) bisa terus mengirim event meskipun consumer sedang sibuk atau down sementara, karena broker (Redis Stream) menyimpan event dalam antrean. Kedua, **skalabilitas konsumsi**: banyak consumer worker dapat membaca dari topic yang sama secara paralel melalui consumer group tanpa mengubah logika publisher. Ketiga, **multiplicity**: satu event dari satu topic secara alami dapat memiliki banyak penerima di masa depan (misalnya menambahkan consumer baru untuk analitik) tanpa mengubah kode publisher sama sekali — sesuatu yang sulit dilakukan pada client-server murni tanpa menambah endpoint baru di sisi server.

### T3 (Bab 3) — At-least-once vs Exactly-once Delivery; Peran Idempotent Consumer

Dalam sistem pengiriman pesan terdistribusi, terdapat tiga garansi pengiriman: at-most-once (pesan mungkin hilang, tidak pernah diulang), at-least-once (pesan mungkin diulang, tidak pernah hilang), dan exactly-once (pesan tepat sekali, idealnya — namun secara teoretis sangat mahal dicapai di lingkungan yang rawan kegagalan jaringan dan proses) (Coulouris et al., 2012).

Pada rancangan ini, publisher menggunakan strategi **at-least-once**: jika publisher tidak menerima konfirmasi dari aggregator (timeout, network blip), ia akan mengirim ulang event yang sama. Ini lebih mudah diimplementasikan dan lebih robust terhadap kegagalan jaringan dibanding mencoba menjamin exactly-once di level transport. Namun, konsekuensinya adalah aggregator **harus** mengasumsikan event bisa datang berulang.

Di sinilah **idempotent consumer** menjadi krusial: consumer dirancang agar memproses event yang sama berkali-kali menghasilkan efek akhir yang identik dengan memprosesnya satu kali. Pada implementasi, idempotency dicapai dengan menjadikan `(topic, event_id)` sebagai kunci unik penyimpanan — operasi `INSERT ... ON CONFLICT DO NOTHING` bersifat idempotent karena percobaan insert kedua, ketiga, dst., tidak mengubah state apa pun selain menaikkan counter `duplicate_dropped`. Dengan demikian, sistem efektif mencapai semantik **effectively-once** di level aplikasi, meski transport-nya tetap at-least-once.

### T4 (Bab 4) — Skema Penamaan Topic dan Event_id untuk Deduplication

Penamaan (naming) dalam sistem terdistribusi harus menjamin bahwa setiap entitas dapat diidentifikasi secara konsisten oleh seluruh komponen sistem, terlepas dari lokasi atau waktu entitas tersebut diakses (Coulouris et al., 2012). Untuk Pub-Sub aggregator ini, dua identitas yang harus dirancang dengan cermat adalah **topic** dan **event_id**.

**Topic** dinormalisasi menjadi lowercase dan menggunakan tanda hubung sebagai pemisah kata (misalnya `user-activity`), mengikuti konvensi penamaan hierarkis yang umum pada sistem pub-sub agar mudah difilter dan tidak ambigu akibat perbedaan kapitalisasi.

**Event_id** wajib bersifat *collision-resistant* — pada implementasi digunakan UUID v4 (122-bit randomness), yang secara praktis menjamin probabilitas collision antar event yang dihasilkan independen oleh publisher berbeda mendekati nol, tanpa memerlukan koordinasi terpusat (seperti auto-increment server tunggal) yang justru akan menjadi bottleneck dan single point of failure. Constraint unik diterapkan pada **kombinasi** `(topic, event_id)`, bukan `event_id` saja, karena dua topic yang berbeda secara logis dapat menggunakan skema ID independen tanpa risiko false-positive dedup antar topic — ini selaras dengan prinsip *transparansi lokasi dan replikasi* di mana penamaan harus tetap benar meski sumber event terdistribusi dan tidak saling mengetahui satu sama lain.

### T5 (Bab 5) — Ordering Praktis (Timestamp + Monotonic Counter); Batasan dan Dampaknya

Pada sistem terdistribusi tanpa clock global yang sinkron sempurna, **logical clock** (seperti Lamport timestamp) atau **vector clock** digunakan untuk menetapkan ordering kausal antar event tanpa bergantung pada waktu fisik (Coulouris et al., 2012). Pendekatan yang lebih sederhana dan praktis untuk aggregator log adalah kombinasi **timestamp ISO8601** (disediakan publisher) dengan **monotonic counter** lokal di sisi aggregator (kolom `id BIGSERIAL` pada tabel `processed_events`, yang bertambah secara monoton sesuai urutan insert berhasil).

Batasan dari pendekatan ini: timestamp yang disertakan publisher **tidak dapat dijadikan acuan total ordering yang ketat**, karena jam pada setiap publisher (clock skew) bisa berbeda beberapa milidetik hingga detik, dan latency jaringan membuat event yang "dikirim lebih dulu" bisa "tiba lebih lambat" di aggregator. Dampaknya: query yang mengandalkan `ORDER BY timestamp` mungkin tidak 100% merefleksikan urutan kejadian sebenarnya di dunia nyata, namun `ORDER BY received_at` (atau `id` auto-increment) menjamin **ordering lokal yang konsisten** di sisi aggregator — cukup untuk kebutuhan log aggregator yang menerima dari banyak sumber independen, di mana total ordering ketat antar semua sumber bukan kebutuhan fungsional utama, melainkan eventual consistency dalam urutan penerimaan yang lebih dipentingkan.

### T6 (Bab 6) — Failure Modes dan Mitigasi (Retry, Backoff, Durable Dedup Store, Crash Recovery)

Kegagalan dalam sistem terdistribusi dapat berupa **omission failure** (pesan/proses hilang), **crash failure** (proses berhenti tanpa peringatan), atau **arbitrary failure** (perilaku tak terduga) (Coulouris et al., 2012). Pada rancangan ini, beberapa failure mode utama dan mitigasinya:

1. **Network failure antara publisher dan aggregator** — dimitigasi dengan retry otomatis di sisi publisher (pola yang lazim pada at-least-once delivery), meskipun ini berarti aggregator harus siap menerima duplikat.
2. **Crash pada consumer worker sebelum ACK** — dimitigasi dengan mekanisme **Pending Entry List (PEL)** pada Redis Stream consumer group: pesan yang belum di-ACK setelah idle time tertentu (30 detik pada implementasi) di-*claim ulang* oleh worker lain via `XAUTOCLAIM`, sehingga proses tidak pernah "hilang" hanya karena satu worker mati.
3. **Crash/restart pada container storage (Postgres)** — dimitigasi dengan **durable dedup store**: tabel `processed_events` disimpan di named volume `pg_data` yang tidak terhapus saat container dihapus/dibuat ulang, sehingga state dedup tetap utuh setelah restart.
4. **Duplikasi akibat retry** — dimitigasi oleh idempotent consumer (lihat T3) yang menjadikan duplikasi sebagai kondisi normal yang ditangani secara aman, bukan dianggap error.

Tidak digunakan exponential backoff eksplisit di publisher pada implementasi saat ini (publisher mengirim ulang segera), namun ini dicatat sebagai *future improvement* yang disarankan buku — backoff penting untuk mencegah *retry storm* yang justru memperparah kondisi saat aggregator sedang overload.

### T7 (Bab 7) — Eventual Consistency pada Aggregator; Peran Idempotency + Dedup

Model konsistensi pada sistem terdistribusi berkisar dari **strong consistency** (semua replika selalu sinkron sebelum operasi dianggap selesai) hingga **eventual consistency** (replika akan konvergen ke state yang sama, namun tidak dijamin instan) (Coulouris et al., 2012). Pub-Sub log aggregator pada proyek ini mengadopsi **eventual consistency** di level sistem: ketika publisher mengirim event, ada jendela waktu singkat di mana event tersebut "dalam perjalanan" — sudah ditulis ke Redis Stream namun belum tentu sudah ter-commit ke Postgres oleh consumer worker.

Idempotency dan deduplication adalah **prasyarat** agar eventual consistency ini tetap aman: tanpa keduanya, retry akibat at-least-once delivery (T3) akan menyebabkan double-counting atau double-processing yang merusak akurasi data begitu sistem akhirnya "settle". Dengan unique constraint `(topic, event_id)` sebagai garis pertahanan terakhir di database, sistem menjamin bahwa **state akhir** (setelah semua retry dan duplikat selesai diproses) akan selalu konvergen ke himpunan event yang benar-benar unik — terlepas dari berapa kali masing-masing event tersebut dikirim ulang sepanjang jalan.

### T8 (Bab 8) — Desain Transaksi: ACID, Isolation Level, dan Strategi Menghindari Lost-Update

Transaksi database menjamin empat properti ACID: **Atomicity** (semua-atau-tidak-sama-sekali), **Consistency** (transaksi membawa database dari satu state valid ke state valid lain), **Isolation** (transaksi konkuren tidak saling mengganggu seolah dieksekusi serial), dan **Durability** (perubahan yang sudah commit tidak akan hilang) (Coulouris et al., 2012).

Pada implementasi, setiap operasi insert event + update statistik dibungkus dalam **satu transaksi tunggal** memakai `AsyncSession` SQLAlchemy: `INSERT INTO processed_events ... RETURNING id` diikuti `UPDATE aggregator_stats SET unique_processed = unique_processed + 1` di-commit bersama. Jika salah satu langkah gagal, seluruh transaksi di-rollback (Atomicity).

**Isolation level yang dipilih: READ COMMITTED** (default PostgreSQL). Alasannya: dedup pada sistem ini tidak bergantung pada *snapshot read* yang konsisten dari banyak baris, melainkan pada *constraint atomik* yang dijamin oleh database engine itu sendiri saat terjadi konflik insert konkuren — dua transaksi yang mencoba INSERT `(topic, event_id)` yang identik secara bersamaan akan menyebabkan salah satu menunggu lock baris unique index dan kemudian "gagal" insert (ON CONFLICT) begitu transaksi pertama commit, terlepas dari isolation level yang dipakai. SERIALIZABLE tidak diperlukan di sini karena tidak ada *write skew* yang mungkin terjadi pada single-row counter update (`count = count + 1` adalah operasi atomik di level row-lock pada READ COMMITTED).

**Strategi menghindari lost-update**: alih-alih membaca nilai counter ke aplikasi, menambah di memori, lalu menulis kembali (pola read-modify-write yang rawan lost-update jika dua transaksi membaca nilai lama yang sama), sistem menggunakan **SQL `UPDATE ... SET count = count + 1 WHERE id = 1`** — operasi ini dieksekusi atomik di sisi database, sehingga PostgreSQL secara internal menyerialkan akses ke baris tersebut (row-level lock), menjamin setiap increment benar-benar terakumulasi meskipun dieksekusi oleh puluhan worker secara bersamaan.

### T9 (Bab 9) — Kontrol Konkurensi: Locking/Unique Constraints/Upsert; Idempotent Write Pattern

Kontrol konkurensi bertujuan menjaga konsistensi data ketika banyak transaksi berjalan bersamaan, melalui mekanisme seperti pessimistic locking (mengunci data sebelum diakses), optimistic concurrency control (mendeteksi konflik saat commit), atau memanfaatkan constraint yang dijamin oleh sistem basis data (Coulouris et al., 2012).

Pada proyek ini, strategi yang dipilih adalah **constraint-based concurrency control** melalui `UNIQUE(topic, event_id)` dikombinasikan dengan pola **idempotent upsert** `INSERT ... ON CONFLICT DO NOTHING`. Pendekatan ini dipilih dibanding pessimistic locking eksplisit (`SELECT ... FOR UPDATE`) karena: (1) lebih ringan — tidak memerlukan transaksi menahan lock untuk durasi check-then-insert yang lebih lama; (2) PostgreSQL menjamin atomicity pemeriksaan-dan-insert dalam satu statement tunggal, menghilangkan jendela race condition antara "cek apakah sudah ada" dan "insert jika belum ada" yang biasa muncul pada pola check-then-act manual.

**Bukti tidak ada double-process**: pada `test_concurrent_workers_no_double_processing` (lihat `tests/test_concurrency_transactions.py`), 10 request paralel mengirim event identik secara bersamaan; hasil pengujian menunjukkan **tepat satu** yang ter-`accepted`, sembilan lainnya terdeteksi `duplicate` — ini secara empiris membuktikan unique constraint berhasil mencegah double-insert di bawah konkurensi nyata, bukan hanya asumsi teoretis.

### T10 (Bab 10–13) — Orkestrasi Compose, Keamanan Jaringan Lokal, Persistensi, Observability

**Orkestrasi (Bab 12–13, sistem berbasis web dan koordinasi):** Docker Compose berperan sebagai *orchestrator* sederhana yang mendefinisikan dependency graph antar service (`depends_on` dengan `condition: service_healthy`), memastikan `aggregator` baru start setelah `storage` dan `broker` benar-benar siap menerima koneksi — bukan hanya "container sudah jalan" tetapi *readiness* yang sesungguhnya diverifikasi lewat health check (`pg_isready`, `redis-cli ping`, endpoint `/health`).

**Keamanan jaringan lokal (Bab 10–11):** Seluruh service berkomunikasi dalam satu Docker bridge network internal bernama `internal`. Hanya `aggregator` yang mem-publish port ke host (`8080:8080`) untuk keperluan akses demo; `broker` (Redis) dan `storage` (Postgres) **tidak memiliki port yang di-expose ke luar Compose**, sehingga tidak dapat diakses langsung dari luar jaringan Docker — mengurangi attack surface sesuai prinsip *least privilege* pada desain sistem berkeamanan, dan memenuhi syarat tugas bahwa sistem tidak boleh mengakses atau diakses oleh layanan eksternal publik.

**Persistensi (sistem berkas/penyimpanan terdistribusi):** Data Postgres disimpan di **named volume** `pg_data`, dan data Redis (untuk crash recovery PEL) di `broker_data` dengan `appendonly yes` diaktifkan. Named volume dikelola oleh Docker secara independen dari lifecycle container, sehingga `docker compose down` (tanpa flag `-v`) atau bahkan `docker compose rm` pada satu service tidak menghapus volume — data tetap tersedia ketika container yang sama atau penggantinya dibuat ulang, sesuai prinsip *durability* dan transparansi penyimpanan terdistribusi.

**Observability:** Endpoint `GET /stats` menyediakan metrik agregat (received, unique_processed, duplicate_dropped, topics, uptime), endpoint `GET /health` berfungsi sebagai *readiness probe*, dan seluruh log aplikasi diformat sebagai JSON terstruktur (`python-json-logger`) agar mudah diparsing oleh tooling log aggregation eksternal di kemudian hari — merealisasikan prinsip *monitoring dan observability* yang ditekankan pada sistem terdistribusi modern berbasis web.

---

## Bagian II — Implementasi: Ringkasan Keputusan Desain

Lihat `README.md` bagian "Asumsi & Keputusan Desain" untuk ringkasan keputusan teknis (bahasa, broker, isolation level, partial commit, jaringan). Bagian ini melengkapi dengan detail tambahan terkait pemenuhan rubrik implementasi.

### Model Event & API
Skema event divalidasi otomatis oleh Pydantic (`app/models.py`) sesuai spesifikasi: `topic`, `event_id`, `timestamp` (ISO8601), `source`, `payload` (bebas). Validasi gagal menghasilkan HTTP 422 dengan detail field yang bermasalah — dibuktikan pada `test_validation.py`.

### Idempotency & Dedup Berlapis
- **Level 1 (Redis SETNX, TTL 24 jam):** fast-path filter untuk mengurangi beban query Postgres pada skenario duplikasi masif (≥30% dari 25.000 event = ±8.750 duplikat).
- **Level 2 (Postgres UNIQUE constraint):** garis pertahanan utama, atomik, dan persisten — tetap benar bahkan jika cache Redis di-reset.

### Transaksi & Konkurensi
Setiap event diproses dalam transaksi independen menggunakan session database tersendiri, sehingga aman dipanggil concurrent lewat `asyncio.gather` tanpa risiko race condition pada level aplikasi (SQLAlchemy `AsyncSession` tidak thread/task-safe jika dibagikan ke banyak coroutine sekaligus). Pembuktian empiris ada di `tests/test_concurrency_transactions.py`.

### Reliability & Ordering
Sistem mengadopsi at-least-once delivery (lihat T3) dengan Redis Stream consumer group sebagai mekanisme crash recovery (PEL + `XAUTOCLAIM`). Total ordering global **tidak** diberlakukan secara ketat (lihat T5) — yang dijamin adalah ordering lokal berdasarkan `received_at`/`id` di aggregator, cukup untuk kebutuhan fungsional log aggregator multi-sumber.

### Performa (Target: ≥20.000 event, ≥30% duplikasi)
Publisher (`publisher/publisher.py`) dikonfigurasi mengirim **25.000 event** dengan **35% rasio duplikasi**, dikirim dalam batch berukuran 50 dengan concurrency 10 klien paralel. Metrik throughput, latency, dan duplicate rate dicatat di log publisher pada akhir eksekusi (`PUBLISH SUMMARY`) dan dapat diverifikasi silang dengan `GET /stats` pada aggregator. Hasil pengujian aktual (throughput, durasi, dan rasio duplikasi terukur) wajib dilampirkan di sini setelah menjalankan `docker compose up` secara penuh di lingkungan masing-masing, karena angka ini bergantung pada spesifikasi mesin yang menjalankan Compose:

| Metrik                     | Nilai (isi setelah run aktual) |
|-----------------------------|----------------------------------|
| Total event terkirim        | 25.000                          |
| Event diterima unik          | _(isi dari /stats: unique_processed)_ |
| Duplikat terdeteksi          | _(isi dari /stats: duplicate_dropped)_ |
| Throughput publisher         | _(isi dari log: events/sec)_    |
| Durasi total publish          | _(isi dari log: elapsed time)_  |

---

## Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Pearson Education.
