import argparse
import json
import sys
import time
import pysolr
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from typing import List, Dict, Any, Tuple

def validate_kafka(kafka_url, kafka_topic, consumer_group):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_url,
            group_id=consumer_group,
            enable_auto_commit=False,
        )
    except Exception as e:
        print(f"ERROR: Unable to connect to Kafka broker at {kafka_url} - {e}")
        return False

    try:
        topics = consumer.topics()
        if kafka_topic not in topics:
            print(f"ERROR: Kafka topic '{kafka_topic}' does not exist. Available topics: {topics}")
            return False
    except Exception as e:
        print(f"ERROR: Unable to retrieve topics from Kafka - {e}")
        return False
    finally:
        consumer.close()

    return True

def validate_solr(solr_url, solr_collection, timeout=5):
    try:
        solr = pysolr.Solr(f"{solr_url.rstrip('/')}/{solr_collection}", timeout=timeout)
        solr.ping()
    except Exception as e:
        print(f"ERROR: Unable to validate Solr at {solr_url} for collection '{solr_collection}' - {e}")
        return False
    return True

def fetch_kafka_batch(kafka_url, kafka_topic, consumer_group, batch_size, timeout_ms=5000):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_url,
        group_id=consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=batch_size,
    )
    try:
        for _ in range(50):
            consumer.poll(timeout_ms=100)
            if consumer.assignment():
                break

        polled = consumer.poll(timeout_ms=timeout_ms)
        batch = []
        for tp, records in polled.items():
            for r in records:
                batch.append({
                    "topic": r.topic,
                    "partition": r.partition,
                    "offset": r.offset,
                    "timestamp": r.timestamp,
                    "key": r.key,
                    "value": r.value,
                    "headers": dict(r.headers) if r.headers else {},
                })
                if len(batch) >= batch_size:
                    break
            if len(batch) >= batch_size:
                break
        return batch
    finally:
        consumer.close()

def commit_processed_messages(kafka_url: str, kafka_topic: str, consumer_group: str, processed_messages: List[Tuple[int, int]]) -> int:
    if not processed_messages:
        return 0

    highest: Dict[int, int] = {}
    for partition, offset in processed_messages:
        prev = highest.get(partition)
        if prev is None or offset > prev:
            highest[partition] = offset

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_url,
        group_id=consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        for _ in range(50):
            consumer.poll(timeout_ms=100)
            if consumer.assignment():
                break
        else:
            raise RuntimeError("Timed out joining consumer group before commit")

        commit_map = {
            TopicPartition(kafka_topic, p): OffsetAndMetadata(o + 1, None, -1)
            for p, o in highest.items()
        }
        if commit_map:
            consumer.commit(offsets=commit_map)
            return len(commit_map)
        return 0
    finally:
        consumer.close()

def index_messages_to_solr(messages: List[Dict[str, Any]], solr_url: str, solr_collection: str, fields: List[str], *, id_field: str | None = None, chunk_size: int, commit: bool = False, soft_commit: bool = False) -> List[Tuple[int, int]]:
    solr = pysolr.Solr(f"{solr_url.rstrip('/')}/{solr_collection}", timeout=10)

    def _to_doc(msg: Dict[str, Any]) -> Dict[str, Any] | None:
        raw = msg.get("value")
        if raw is None:
            return None
        try:
            payload = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
        except Exception:
            return None

        if isinstance(payload, dict) and "resolved" in payload:
            print(f"[resolved] partition={msg.get('partition')} offset={msg.get('offset')} value={payload['resolved']}")
            return None

        after = payload.get("after") if isinstance(payload, dict) else None
        if not isinstance(after, dict):
            return None

        doc = {k: after.get(k) for k in fields if k in after}

        if id_field is not None and id_field not in doc:
            key = msg.get("key")
            doc[id_field] = key.decode() if isinstance(key, (bytes, bytearray)) else key

        return doc

    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    successes: List[Tuple[int, int]] = []

    for m in messages:
        result = _to_doc(m)
        if result is not None and not isinstance(result, dict):
            # if _to_doc returned None, it's a resolved or skipped message
            continue
        elif isinstance(result, dict):
            pairs.append((m, result))

    for i in range(0, len(pairs), chunk_size):
        chunk = pairs[i : i + chunk_size]
        docs = [d for _, d in chunk]
        try:
            solr.add(docs, commit=commit, softCommit=soft_commit, overwrite=True)
            successes.extend((m["partition"], m["offset"]) for m, _ in chunk)
        except Exception:
            for m, d in chunk:
                try:
                    solr.add([d], commit=False, softCommit=False, overwrite=True)
                    successes.append((m["partition"], m["offset"]))
                except Exception:
                    pass

    # If no successes (no indexable docs), still return highest seen offsets to commit
    if not successes:
        highest_seen: Dict[int, int] = {}
        for m in messages:
            p, o = m.get("partition"), m.get("offset")
            if p is None or o is None:
                continue
            prev = highest_seen.get(p)
            if prev is None or o > prev:
                highest_seen[p] = o
        return list(highest_seen.items())

    if commit:
        try:
            solr.commit(softCommit=soft_commit)
        except Exception:
            pass

    return successes

def main():
    parser = argparse.ArgumentParser(description="Consume CDC messages from Kafka and populate Solr.")
    parser.add_argument("-k", "--kafka-url", required=True)
    parser.add_argument("-t", "--kafka-topic", required=True)
    parser.add_argument("-s", "--solr-url", required=True)
    parser.add_argument("-c", "--solr-collection", required=True)
    parser.add_argument("-g", "--kafka-consumer-group", required=True)
    parser.add_argument("-b", "--batch-size", type=int, default=100)
    parser.add_argument("-n", "--num-batches", type=int, default=None)
    parser.add_argument("-i", "--batch-interval-ms", type=int, default=0)
    parser.add_argument("-f", "--fields", nargs="+", required=True)
    args = parser.parse_args()

    if not validate_kafka(args.kafka_url, args.kafka_topic, args.kafka_consumer_group):
        sys.exit(-1)
    if not validate_solr(args.solr_url, args.solr_collection):
        sys.exit(-1)

    total_processed = 0
    batch_num = 0

    while True:
        if args.num_batches is not None and batch_num >= args.num_batches:
            break

        batch = fetch_kafka_batch(args.kafka_url, args.kafka_topic, args.kafka_consumer_group, args.batch_size)

        if not batch:
            print(f"[batch {batch_num+1}] No more messages to process.")
            break

        processed_messages = index_messages_to_solr(
            messages=batch,
            solr_url=args.solr_url,
            solr_collection=args.solr_collection,
            fields=args.fields,
            chunk_size=args.batch_size,
            commit=True,
        )

        print(f"[batch {batch_num+1}] processed: {processed_messages}")

        committed_partitions = commit_processed_messages(
            kafka_url=args.kafka_url,
            kafka_topic=args.kafka_topic,
            consumer_group=args.kafka_consumer_group,
            processed_messages=processed_messages,
        )
        print(f"[batch {batch_num+1}] committed offsets for {committed_partitions} partitions")

        total_processed += len(processed_messages)
        batch_num += 1

        if args.batch_interval_ms > 0 and (args.num_batches is None or batch_num < args.num_batches):
            time.sleep(args.batch_interval_ms / 1000.0)

    print(f"Total messages indexed: {total_processed}")

if __name__ == "__main__":
    main()
