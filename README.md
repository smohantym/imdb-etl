# IMDb Movie Dataset ETL — Kafka + Spark (Docker)

Stream the public IMDb datasets into **Kafka**, transform with **Spark Structured Streaming** to compute:
- **Bayesian weighted movie ratings** (per movie)
- **Genre counts** (running snapshot)

Outputs are written as **Parquet** (for analytics) and **CSV** (for quick readability).

---

## 🧱 Architecture

```

IMDb TSV.gz  ──(requests+pandas)──>  Producer  ──JSON──>  Kafka(topic: imdb-movies)
|
v
Spark Structured Streaming
├─ Silver (append Parquet)
└─ Genre counts (snapshot: Parquet + CSV)

```

- **Zookeeper** for Kafka (Confluent images).
- **Kafka** topic `imdb-movies` (3 partitions).
- **Producer** downloads & merges IMDb dumps and streams JSON rows.
- **Spark** reads from Kafka and writes lake outputs to host-mounted `data/output`.

---

## 📁 Project Structure

```

.
├─ docker-compose.yml
├─ .env
├─ data/
│  └─ output/
│     ├─ parquet/
│     │  ├─ silver_movies/
│     │  └─ genre_counts/
│     └─ csv/
│        └─ genre_counts/
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
└─ spark/
└─ app.py

````

---

## 🔧 Prerequisites

- Docker & Docker Compose v2
- ~4 GB free RAM
- Internet access (to download IMDb TSV dumps inside the producer)

---

## ⚙️ Configuration

Create `.env` in the project root:

```env
KAFKA_TOPIC=imdb-movies
MSGS_PER_SEC=10
GLOBAL_MEAN_RATING=6.5
MIN_VOTES_PRIOR=3000
# Optional for quick testing: limit number of rows the producer sends
# PRODUCER_LIMIT=500
````

> The compose file uses `${KAFKA_TOPIC:-events}` defaults; the `.env` above ensures **all services** agree on `imdb-movies`.

---

## 🚀 Quick Start

```bash
# From the project root (where docker-compose.yml lives)

# 1) Clean reset (optional, but recommended the first time)
docker compose down -v --remove-orphans
rm -rf data/output/parquet/_chk

# 2) Build and start everything
docker compose up --build -d

# 3) Watch logs
docker compose logs --no-color kafka-init | sed -n '1,200p'
docker compose logs --no-color producer | sed -n '1,200p'
docker compose logs --no-color spark-streaming | sed -n '1,200p'
```

**UIs**

* Spark Master: [http://localhost:8080](http://localhost:8080)
* Spark Worker: [http://localhost:8081](http://localhost:8081)

---

## 🔄 What Each Service Does

### `zookeeper`

* Backing service required by the Confluent Kafka image.

### `kafka`

* Broker with two listeners:

  * `kafka:9092` inside Docker network (for Spark & Producer)
  * `localhost:9094` on host (optional for host tools)
* Persists data in the named volume `kafka-data`.

### `kafka-init`

* One-shot job that **creates** the topic `${KAFKA_TOPIC}` (default `imdb-movies`) with **3 partitions**.

### `producer`

* Python container (`kafka-python`, `pandas`, `requests`).
* Downloads `title.basics.tsv.gz` & `title.ratings.tsv.gz`, merges & filters to movies.
* Streams JSON rows to Kafka at `MSGS_PER_SEC`.

### `spark-streaming`

* Waits for `kafka:9092` to be reachable, then `spark-submit`s `spark/app.py`.
* Reads Kafka JSON → computes `weighted_score` → writes:

  * **Silver** movies table (append Parquet)
  * **Genre counts snapshot** (overwrite Parquet + single-file CSV)
* Uses `RUN_ID` to namespace checkpoints per run.

---

## 📦 Outputs

On your **host** (mounted from containers):

* **Parquet (append)**
  `data/output/parquet/silver_movies/`

* **Parquet (snapshot – overwritten per micro-batch)**
  `data/output/parquet/genre_counts/`

* **CSV (snapshot – overwritten per micro-batch)**
  `data/output/csv/genre_counts/`

> The CSV snapshot is for readability (coalesced to one file per batch).

---

## 🔍 Verifying Data

List the output folders:

```bash
ls -R data/output/parquet
ls -R data/output/csv || true
```

Quick peek using Python:

```bash
python - <<'PY'
import glob, pandas as pd
silver = glob.glob("data/output/parquet/silver_movies/*.parquet")
print("silver parquet files:", len(silver))
if silver:
    print(pd.read_parquet(silver[0]).head(5))

csvs = glob.glob("data/output/csv/genre_counts/*.csv")
print("genre_counts csv files:", len(csvs))
if csvs:
    print(pd.read_csv(csvs[0]).head(10))
PY
```

(Or use DuckDB, Spark, or your favorite Parquet viewer.)

---

## 🧪 Common Commands

**Check Kafka topic**

```bash
docker compose exec kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --list"
docker compose exec kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --describe --topic ${KAFKA_TOPIC:-imdb-movies}"
```

**Tail logs**

```bash
docker compose logs -f producer
docker compose logs -f spark-streaming
```

**Restart app containers (after code changes)**

```bash
docker compose up -d --build producer spark-streaming
```

---

## 🛠️ Tuning Tips

* **Throughput**: increase `MSGS_PER_SEC` in `.env`.
* **Weighted score priors**: tune `GLOBAL_MEAN_RATING`, `MIN_VOTES_PRIOR`.
* **Shuffle partitions**: increase `spark.sql.shuffle.partitions` for larger loads.
* **Checkpoints**: changing topic name/partitions? Clear `data/output/parquet/_chk/` or start with a fresh `RUN_ID`.

---

## 🧯 Troubleshooting

### 1) `UnknownTopicOrPartitionException`

* Ensure **all services** use the same topic (`.env` → `KAFKA_TOPIC=imdb-movies`).
* Delete Spark checkpoints: `rm -rf data/output/parquet/_chk`
* Confirm topic exists & has 3 partitions:

  ```bash
  docker compose exec kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --describe --topic imdb-movies"
  ```

### 2) `cluster.id` mismatch on Kafka startup

* The broker volume is out of sync with Zookeeper. Reset:

  ```bash
  docker compose down -v --remove-orphans
  docker volume ls | grep kafka-data || true
  # if still present: docker volume rm <name>
  ```
* Re-create: `docker compose up -d zookeeper kafka`

### 3) Producer: `KafkaTimeoutError: Failed to allocate memory...`

* It’s local buffer backpressure while waiting for broker/metadata.
* Fixed by:

  * Waiting for topic metadata before send (already in code).
  * Lower `MSGS_PER_SEC` (e.g., 2) then increase later.
  * Using larger `buffer_memory` / `max_block_ms`.

### 4) Spark starts before Kafka

* `spark-streaming` service **actively waits for TCP** on `kafka:9092` in the entrypoint.
* If you altered the compose, keep that wait loop.

### 5) Genre snapshot “disappears”

* Happens if an **empty** micro-batch overwrote the snapshot.
* Code now **skips empty batches**, so the snapshot persists.

---

## 📜 Notes

* IMDb datasets are downloaded from `https://datasets.imdbws.com/` inside the producer container.
* This project is for educational/demo purposes—observe IMDb’s terms of use for any downstream publication.

---

## 🧹 Stop / Reset

```bash
# Stop containers, keep data volumes
docker compose down

# Full reset (containers + named volumes + checkpoints)
docker compose down -v --remove-orphans
rm -rf data/output/parquet/_chk
```