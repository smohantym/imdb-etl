import os
import time
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.utils import StreamingQueryException

# ----------------- ENV -----------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
OUTPUT_PATH_CSV = os.getenv("OUTPUT_PATH_CSV", "/opt/spark-output/csv")  # for readable CSV snapshot

# Priors for Bayesian weighted rating
C = float(os.getenv("GLOBAL_MEAN_RATING", "6.5"))
M = float(os.getenv("MIN_VOTES_PRIOR", "3000"))

# New checkpoint namespace per run (avoids stale offsets)
RUN_ID = os.getenv("RUN_ID", str(int(time.time())))

# ----------------- SPARK -----------------
spark = (
    SparkSession.builder.appName("IMDbMoviesStreamingETL")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")  # AQE not supported in streaming
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"[BOOT] KAFKA_TOPIC={KAFKA_TOPIC}  BOOTSTRAP={KAFKA_BOOTSTRAP_SERVERS}  RUN_ID={RUN_ID}")

# ----------------- SCHEMA -----------------
schema = T.StructType([
    T.StructField("tconst", T.StringType()),
    T.StructField("primaryTitle", T.StringType()),
    T.StructField("startYear", T.IntegerType()),
    T.StructField("runtimeMinutes", T.IntegerType()),
    T.StructField("genres", T.ArrayType(T.StringType())),
    T.StructField("averageRating", T.DoubleType()),
    T.StructField("numVotes", T.IntegerType())
])

# ----------------- SOURCE (Kafka) -----------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")   # full rebuild each run
    .option("failOnDataLoss", "false")
    .load()
)

json_df = raw.select(F.col("value").cast("string").alias("json"))
movies_df = json_df.select(F.from_json("json", schema).alias("d")).select("d.*")

# ----------------- ENRICH -----------------
# Weighted rating: WR = (v/(v+M))*R + (M/(v+M))*C
movies_with_score = movies_df.withColumn(
    "weighted_score",
    (F.col("numVotes") / (F.col("numVotes") + F.lit(M))) * F.col("averageRating")
    + (F.lit(M) / (F.col("numVotes") + F.lit(M))) * F.lit(C)
)

# ----------------- SINK 1: Silver (append Parquet) -----------------
silver_path = os.path.join(OUTPUT_PATH, "silver_movies")
silver_chk  = os.path.join(OUTPUT_PATH, "_chk", f"silver_movies_{RUN_ID}")

silver_q = (
    movies_with_score.writeStream
    .format("parquet")
    .option("path", silver_path)
    .option("checkpointLocation", silver_chk)
    .outputMode("append")
    .trigger(processingTime="20 seconds")
    .start()
)

# ----------------- SINK 2: Genre counts (direct Parquet + CSV, no staging) -----------------
genres_df = movies_with_score.select(F.explode_outer("genres").alias("genre")).na.fill({"genre": "(unknown)"})
genre_counts_df = genres_df.groupBy("genre").agg(F.count("*").alias("movie_count"))

genre_parquet_path = os.path.join(OUTPUT_PATH, "genre_counts")
genre_csv_path     = os.path.join(OUTPUT_PATH_CSV, "genre_counts")
genre_chk          = os.path.join(OUTPUT_PATH, "_chk", f"genre_counts_{RUN_ID}")

def write_full_counts(df, epoch_id: int):
    # Skip empty aggregates so we don't wipe snapshots when no new data arrives
    if df.rdd.isEmpty():
        print(f"[foreachBatch] epoch={epoch_id} empty aggregate — skipping write")
        return

    out = df.orderBy(F.desc("movie_count"), F.asc("genre"))
    rows = out.count()
    print(f"[foreachBatch] epoch={epoch_id} writing {rows} rows → {genre_parquet_path} (parquet) & {genre_csv_path} (csv)")

    # Direct overwrite of final destinations (no staging dirs)
    out.write.mode("overwrite").parquet(genre_parquet_path)
    (out.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(genre_csv_path))

counts_q = (
    genre_counts_df.writeStream
    .outputMode("complete")                     # full aggregate each micro-batch
    .option("checkpointLocation", genre_chk)
    .foreachBatch(write_full_counts)
    .start()
)

# ----------------- RUN -----------------
def dump_query_state(q):
    try:
        lp = q.lastProgress or {}
    except Exception:
        lp = {}
    print(f"[STREAM] name={q.name} id={q.id} isActive={q.isActive}")
    if lp:
        print("[LAST_PROGRESS]", lp)

try:
    print("[BOOT] Streams started. Waiting for termination …")
    spark.streams.awaitAnyTermination()
except StreamingQueryException as e:
    print("[ERROR] StreamingQueryException:", e)
    for q in spark.streams.active:
        dump_query_state(q)
    raise
except Exception as e:
    print("[ERROR] Exception:", e)
    for q in spark.streams.active:
        dump_query_state(q)
    raise
