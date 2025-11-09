import os, io, gzip, json, time, logging, sys
import requests, pandas as pd
from kafka import KafkaProducer, errors as kerrors

BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "5"))
PRODUCER_LIMIT = os.getenv("PRODUCER_LIMIT")  # e.g., "500" for quick test

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("imdb-producer")

def _download_gz(url):
    logger.info(f"Downloading {url} ...")
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        df = pd.read_csv(gz, sep="\t", dtype=str, na_values=["\\N"], low_memory=False)
    logger.info(f"Loaded {url.split('/')[-1]} with {len(df):,} rows")
    return df

def prepare_records(limit=None):
    basics = _download_gz(BASICS_URL)
    ratings = _download_gz(RATINGS_URL)
    df = basics.merge(ratings, on="tconst", how="inner")
    df = df[df["titleType"] == "movie"]
    df = df[df["startYear"].notna()]
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").fillna(0).astype(int)
    df = df[df["numVotes"] >= 1000]
    df["genres"] = df["genres"].fillna("").apply(lambda g: [x for x in g.split("|") if x])
    if limit:
        df = df.head(limit)
    cols = ["tconst","primaryTitle","startYear","runtimeMinutes","genres","averageRating","numVotes"]
    df = df[cols]
    logger.info(f"Prepared filtered movie dataset with {len(df):,} rows")
    return df

def wait_for_topic(producer: KafkaProducer, topic: str, timeout_s: int = 60):
    logger.info(f"Waiting for topic metadata: {topic}")
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            parts = producer.partitions_for(topic)
            if parts:
                logger.info(f"Topic {topic} is ready with partitions: {sorted(parts)}")
                return
        except Exception as e:
            logger.warning(f"Metadata fetch failed: {e}")
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for topic metadata for '{topic}'")

def main():
    limit = int(PRODUCER_LIMIT) if PRODUCER_LIMIT else None
    rate = max(MSGS_PER_SEC, 0.1)
    delay = 1.0 / rate

    logger.info(f"Starting producer to topic='{KAFKA_TOPIC}' at '{KAFKA_BOOTSTRAP_SERVERS}' "
                f"rate={rate}/s limit={limit}")

    # Roomier buffers + compression + longer max_block
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        acks=1,  # <-- int, not "1"
        linger_ms=100,
        batch_size=32 * 1024,
        buffer_memory=128 * 1024 * 1024,
        max_block_ms=120000,
        retries=5,
        retry_backoff_ms=500,
        compression_type="gzip",
        request_timeout_ms=30000,
    )

    # Ensure topic metadata is available before sending
    wait_for_topic(producer, KAFKA_TOPIC, timeout_s=90)

    df = prepare_records(limit=limit)
    sent = 0
    try:
        for _, rec in df.iterrows():
            try:
                producer.send(KAFKA_TOPIC, rec.to_dict())
                sent += 1
                # Backpressure: flush every 500 records
                if sent % 500 == 0:
                    producer.flush()
                    logger.info(f"sent {sent:,} messages…")
                time.sleep(delay)
            except kerrors.KafkaTimeoutError as e:
                logger.warning(f"KafkaTimeoutError on send (sent={sent:,}): {e}. Flushing and retrying…")
                producer.flush()
                time.sleep(1.0)
            except Exception as e:
                logger.exception(f"Unexpected error on send (sent={sent:,}): {e}")
                time.sleep(1.0)
        # final flush
        producer.flush()
        logger.info(f"Done. Sent {sent:,} messages")
    finally:
        producer.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
