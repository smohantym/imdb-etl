# 1) `docker-compose.yml` — Services & wiring

## Zookeeper

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.3.arm64
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    healthcheck: ...
```

* Runs Zookeeper (needed by this Kafka image).
* Exposes 2181 for Kafka to connect.
* Healthcheck waits until ZK responds to simple shell command.

## Kafka broker

```yaml
  kafka:
    image: confluentinc/cp-kafka:7.9.3.arm64
    depends_on:
      zookeeper: { condition: service_healthy }
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://:9092,PLAINTEXT_HOST://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
      ...
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck: ...
```

* One Kafka broker.
* **Two listeners**:

  * `PLAINTEXT://kafka:9092` for **in-cluster** clients (Spark, producer).
  * `PLAINTEXT_HOST://localhost:9094` for **host** tools (optional).
* `kafka-data` persists broker metadata; that’s why resetting it fixed the cluster.id mismatch.
* Healthcheck uses `kafka-topics --list` to know broker is ready.

## One-shot topic initializer

```yaml
  kafka-init:
    image: confluentinc/cp-kafka:7.9.3.arm64
    depends_on: { kafka: { condition: service_healthy } }
    entrypoint: [ "/bin/bash","-lc" ]
    command: >
      set -euo pipefail;
      for i in {1..30}; do
        if /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then break; fi; sleep 2; done;
      /usr/bin/kafka-topics --bootstrap-server kafka:9092 \
        --create --if-not-exists --topic "${KAFKA_TOPIC:-events}" --partitions 3 --replication-factor 1;
      /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list
```

* Waits for Kafka to accept admin ops, then **creates** `${KAFKA_TOPIC}` (defaults to `events` if `.env` is missing).
* Exits once done. Your `.env` sets `KAFKA_TOPIC=imdb-movies`, so that topic is created with **3 partitions**.

## Spark master & worker

```yaml
  spark-master:
    image: spark:3.5.1-python3
    command: ["spark-class","org.apache.spark.deploy.master.Master","--host","spark-master"]
    ports: ["7077:7077","8080:8080"]
    volumes: ["./data/output:/opt/spark-output"]

  spark-worker:
    image: spark:3.5.1-python3
    depends_on: [spark-master]
    command: ["spark-class","org.apache.spark.deploy.worker.Worker","spark://spark-master:7077"]
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    ports: ["8081:8081"]
    volumes: ["./data/output:/opt/spark-output"]
```

* Master opens the cluster; worker registers with master at `spark://spark-master:7077`.
* UIs: master at `http://localhost:8080`, worker at `http://localhost:8081`.
* Mount `./data/output` into containers as `/opt/spark-output` so you can see Parquet on the host.

## Spark Structured Streaming job

```yaml
  spark-streaming:
    image: spark:3.5.1-python3
    depends_on:
      kafka: { condition: service_healthy }
      kafka-init: { condition: service_completed_successfully }
      spark-master: { condition: service_started }
    restart: on-failure
    volumes:
      - ./spark:/opt/spark-app
      - ./data/output:/opt/spark-output
      - ./cache/ivy:/tmp/.ivy2
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
      - OUTPUT_PATH=/opt/spark-output/parquet
      - HOME=/tmp
      - RUN_ID=${RUN_ID:-$$(date +%s)}
    entrypoint: |
      set -euo pipefail
      echo "[spark-streaming] waiting for TCP on kafka:9092 ..."
      for i in {1..60}; do
        if timeout 1 bash -lc ':</dev/tcp/kafka/9092' 2>/dev/null; then
          echo "[spark-streaming] kafka:9092 is reachable."; break; fi
        echo "[spark-streaming] still waiting ($$i/60) ..."; sleep 2; done
      mkdir -p /opt/spark-output /tmp/.ivy2; chmod -R 777 /opt/spark-output /tmp/.ivy2 || true
      exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/spark-app/app.py
```

* **Active TCP wait** for `kafka:9092` to avoid racing the broker (no Kafka CLI needed).
* Uses env vars mounted from `.env` (Compose substitution) to pass topic and output paths into the job.
* `RUN_ID` namespaces checkpoints per run, avoiding stale offset/partition metadata.

## Producer

```yaml
  producer:
    build: { context: ./producer }
    depends_on:
      kafka: { condition: service_healthy }
      kafka-init: { condition: service_completed_successfully }
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
      - MSGS_PER_SEC=${MSGS_PER_SEC:-5}
    command: ["python","producer.py"]
```

* Simple Python container built from your `producer/` folder.
* Streams IMDb rows as JSON to the Kafka topic at a controlled rate.

---

# 2) `.env` — Runtime knobs

```env
KAFKA_TOPIC=imdb-movies      # standardize topic across all services
MSGS_PER_SEC=10              # producer throughput
GLOBAL_MEAN_RATING=6.5       # Spark prior C
MIN_VOTES_PRIOR=3000         # Spark prior M
```

* Compose injects these into containers. In Python, your code uses `os.getenv("KAFKA_TOPIC", "events")` so `"events"` is a fallback only if `.env` is missing.
* Keep `.env` consistent to avoid “UnknownTopicOrPartition” and similar issues.

---

# 3) `producer/` — Build & stream IMDb rows

## `producer/Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]
```

* Minimal Python image; installs dependencies; runs your script.

## `producer/requirements.txt`

```
kafka-python==2.0.2
pandas==2.2.3
requests==2.32.3
python-dateutil==2.9.0.post0
```

* `kafka-python` client, `pandas` for TSV handling, `requests` to download IMDb dumps.

## `producer/producer.py` (line-by-line essentials)

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "5"))
PRODUCER_LIMIT = os.getenv("PRODUCER_LIMIT")  # optional cap for testing
```

* Reads runtime config from env; defaults ensure the container can still run.

```python
def _download_gz(url):
    r = requests.get(url, timeout=180); r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        df = pd.read_csv(gz, sep="\t", dtype=str, na_values=["\\N"], low_memory=False)
    return df
```

* Downloads IMDb TSV.GZ, reads into a DataFrame.
* `dtype=str` keeps all columns as strings, then you coerce numerics you care about.

```python
def prepare_records(limit=None):
    basics = _download_gz(BASICS_URL)
    ratings = _download_gz(RATINGS_URL)
    df = basics.merge(ratings, on="tconst", how="inner")
    df = df[df["titleType"] == "movie"]
    df = df[df["startYear"].notna()]
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").fillna(0).astype(int)
    df = df[df["numVotes"] >= 1000]  # quality filter
    df["genres"] = df["genres"].fillna("").apply(lambda g: [x for x in g.split("|") if x])
    if limit: df = df.head(limit)
    cols = ["tconst","primaryTitle","startYear","runtimeMinutes","genres","averageRating","numVotes"]
    return df[cols]
```

* Joins basics+ratings, filters to real movies with some votes, normalizes `genres` to list[str].
* Returns a tidy schema your Spark app expects.

```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
    acks=1,                 # **int** (you fixed earlier)
    linger_ms=100,          # micro-batching for throughput
    batch_size=32 * 1024,   # ~32KB batch
    buffer_memory=128 * 1024 * 1024,  # room to buffer
    max_block_ms=120000,    # avoid KafkaTimeoutError
    retries=5,
    retry_backoff_ms=500,
    compression_type="gzip",
    request_timeout_ms=30000,
)
```

* Balanced producer tuning: small batching, room to buffer, gzip compression, retries.

```python
wait_for_topic(producer, KAFKA_TOPIC, timeout_s=90)
```

* Ensures metadata is visible (partitions discovered) before sending, preventing initial backpressure timeouts.

```python
for _, rec in df.iterrows():
    producer.send(KAFKA_TOPIC, rec.to_dict())
    if sent % 500 == 0: producer.flush()
    time.sleep(1.0 / rate)
```

* Emits JSON messages at your target rate, flushing periodically.
* When done, final `flush()` ensures all in-flight messages are delivered.

**What you’ll see in logs**

* “Downloading … Loaded … Prepared …”
* “Topic imdb-movies is ready with partitions: [0,1,2]”
* “sent 500 messages…”

---

# 4) `spark/app.py` — Streaming ETL

## Boot & schema

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
OUTPUT_PATH_CSV = os.getenv("OUTPUT_PATH_CSV", "/opt/spark-output/csv")
C = float(os.getenv("GLOBAL_MEAN_RATING", "6.5"))
M = float(os.getenv("MIN_VOTES_PRIOR", "3000"))
RUN_ID = os.getenv("RUN_ID", str(int(time.time())))
```

* Reads configs; `RUN_ID` isolates checkpoints per run to avoid stale offset/partition issues.

```python
spark = (SparkSession.builder
    .appName("IMDbMoviesStreamingETL")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate())
```

* Disables AQE since Spark Structured Streaming doesn’t support it.
* Modest shuffle partition count appropriate for a single worker, tunable later.

## Source: Kafka → JSON → columns

```python
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")   # recompute from the beginning
    .option("failOnDataLoss", "false")
    .load())

json_df = raw.select(F.col("value").cast("string").alias("json"))
movies_df = json_df.select(F.from_json("json", schema).alias("d")).select("d.*")
```

* Reads the topic as a stream, starts from the beginning for reproducibility.
* JSON is parsed into typed columns using your schema.

## Transform: Bayesian weighted score

```python
movies_with_score = movies_df.withColumn(
  "weighted_score",
  (F.col("numVotes") / (F.col("numVotes") + F.lit(M))) * F.col("averageRating")
  + (F.lit(M) / (F.col("numVotes") + F.lit(M))) * F.lit(C)
)
```

* IMDb-style smoothing: movies with few votes lean toward global mean `C`, heavy-voted titles reflect their rating.

## Sink 1: Silver (append Parquet)

```python
silver_path = os.path.join(OUTPUT_PATH, "silver_movies")
silver_chk  = os.path.join(OUTPUT_PATH, "_chk", f"silver_movies_{RUN_ID}")

silver_q = (movies_with_score.writeStream
    .format("parquet")
    .option("path", silver_path)
    .option("checkpointLocation", silver_chk)
    .outputMode("append")
    .trigger(processingTime="20 seconds")
    .start())
```

* Appends rows as they come (micro-batches every ~20s).
* Checkpoints are isolated by `RUN_ID` to avoid reuse problems.

## Sink 2: Genre counts snapshot (Parquet + CSV, no staging)

```python
genres_df = movies_with_score.select(F.explode_outer("genres").alias("genre")).na.fill({"genre": "(unknown)"})
genre_counts_df = genres_df.groupBy("genre").agg(F.count("*").alias("movie_count"))

genre_parquet_path = os.path.join(OUTPUT_PATH, "genre_counts")
genre_csv_path     = os.path.join(OUTPUT_PATH_CSV, "genre_counts")
genre_chk          = os.path.join(OUTPUT_PATH, "_chk", f"genre_counts_{RUN_ID}")
```

* **Flatten** genres into one per row, group to get counts.

```python
def write_full_counts(df, epoch_id: int):
    if df.rdd.isEmpty():  # <-- avoids wiping output on empty batch
        print(f"[foreachBatch] epoch={epoch_id} empty aggregate — skipping write"); return
    out = df.orderBy(F.desc("movie_count"), F.asc("genre"))
    print(f"[foreachBatch] epoch={epoch_id} writing {out.count()} rows → {genre_parquet_path} & {genre_csv_path}")
    out.write.mode("overwrite").parquet(genre_parquet_path)            # **no staging**
    (out.coalesce(1).write.mode("overwrite").option("header","true").csv(genre_csv_path))
```

* `foreachBatch` lets you treat each micro-batch like a normal DataFrame:

  * **Overwrite** the Parquet snapshot with the full counts.
  * Also emit a **single-file CSV** for easy eyeballing.
  * Skip the write on empty batch to avoid “auto-delete” behavior.

```python
counts_q = (genre_counts_df.writeStream
    .outputMode("complete")                   # full aggregate per batch
    .option("checkpointLocation", genre_chk)
    .foreachBatch(write_full_counts)
    .start())
```

* Uses `complete` mode because we want the **entire** counts snapshot each batch.

## Run & diagnostics

```python
spark.streams.awaitAnyTermination()
```

* Blocks the container while the two queries (`silver_q`, `counts_q`) run.
* Extra diagnostic blocks will print query state if an exception surfaces.

---

# 5) Runtime workflow (what happens when you run it)

1. `docker compose up --build -d`

   * Zookeeper starts → Kafka starts → healthcheck passes.
   * `kafka-init` creates `imdb-movies` with 3 partitions.
   * Spark master/worker start.
   * `spark-streaming` waits until `kafka:9092` is reachable, then submits `app.py`.
   * `producer` starts, downloads IMDb dumps, prepares the DataFrame.

2. Producer emits JSON to Kafka

   * Checks topic metadata (partitions discovered).
   * Sends messages at `MSGS_PER_SEC` (e.g., 10/s); flushes periodically.

3. Spark consumes and writes

   * `silver_movies/` gets appended Parquet files every 20s.
   * `genre_counts/` snapshot (Parquet + CSV) is **overwritten** each micro-batch that has data.
   * If a batch has no data → snapshot **not touched** (so no accidental deletes).

4. Where to look

   * Parquet:

     * `data/output/parquet/silver_movies/`
     * `data/output/parquet/genre_counts/`
   * CSV:

     * `data/output/csv/genre_counts/`
   * UIs:

     * Spark master: `http://localhost:8080`
     * Spark worker: `http://localhost:8081`

---

# 6) Useful commands & quick checks

* **List topics / describe**

  ```bash
  docker compose exec kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --list"
  docker compose exec kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --describe --topic imdb-movies"
  ```

* **See producer progress**

  ```bash
  docker compose logs -f producer | sed -n '1,200p'
  ```

* **See streaming logs**

  ```bash
  docker compose logs -f spark-streaming | sed -n '1,200p'
  ```