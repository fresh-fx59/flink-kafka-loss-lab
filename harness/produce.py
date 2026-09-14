#!/usr/bin/env python3
"""Deterministic producer.

Emits tab-separated records so the job needs no JSON dependency:

    event_id \t produced_at_ms \t route \t payload

and writes its own ledger to out/<run>/produced.jsonl. THE LEDGER, NOT KAFKA, IS THE
SOURCE OF TRUTH for "what should have arrived" - the verifier never asks Kafka what it
holds, because that is the thing under test.
"""
import argparse, json, os, sys, time
from kafka import KafkaProducer

ROUTES = ("a", "b")


def route_for(event_id: int) -> str:
    # One 'rare' event per 500, so t_rare is idle for long stretches.
    if event_id % 500 == 0:
        return "rare"
    return ROUTES[event_id % 2]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--start-id", type=int, required=True)
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--rate", type=float, default=50.0, help="events per second")
    p.add_argument("--ledger", required=True)
    p.add_argument("--phase", required=True,
                   help="before-outage | during-outage | after-restart")
    p.add_argument("--timestamp-shift-ms", type=int, default=0,
                   help="shift the RECORD timestamp by this many ms (negative = "
                        "back-date). Only has an effect on a topic with "
                        "message.timestamp.type=CreateTime, where the producer's "
                        "clock is what lands in the log and in the time index.")
    args = p.parse_args()

    os.makedirs(os.path.dirname(args.ledger), exist_ok=True)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        acks="all",
        linger_ms=5,
        retries=10,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    written = 0
    with open(args.ledger, "a", encoding="utf-8") as fh:
        for i in range(args.count):
            eid = args.start_id + i
            now_ms = int(time.time() * 1000)
            route = route_for(eid)
            value = f"{eid}\t{now_ms}\t{route}\tpayload-{eid}"
            # Key by event_id so a given id always lands on the same partition.
            # timestamp_ms is the RECORD timestamp; under CreateTime it is what the
            # time index is built from, so back-dating it is exactly the producer-skew
            # case that makes offsetsForTimes answer wrongly.
            producer.send(args.topic, key=str(eid).encode("utf-8"), value=value,
                          timestamp_ms=now_ms + args.timestamp_shift_ms)
            fh.write(json.dumps({
                "event_id": eid, "produced_at": now_ms,
                "route": route, "phase": args.phase,
                "record_ts": now_ms + args.timestamp_shift_ms,
            }) + "\n")
            written += 1
            if interval:
                time.sleep(interval)
    producer.flush(timeout=60)
    producer.close(timeout=30)
    print(f"produced {written} events ({args.phase}) ids "
          f"{args.start_id}..{args.start_id + args.count - 1}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
