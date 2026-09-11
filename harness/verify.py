#!/usr/bin/env python3
"""Verdict computation.

For every destination table t:

    missing[t]   = expected_ids[t] - actual_ids[t]      MUST be empty
    duplicate[t] = ids appearing more than once         reported; fatal only when the
                                                        sink mode promises uniqueness
    extra[t]     = actual_ids[t] - expected_ids[t]      MUST be empty

Row counts and Kafka consumer lag are NOT evidence and are deliberately not computed.

The headline number the operator asked for is `outage.saved` vs `outage.produced`:
of the events produced while the job was down, how many are now in the database.
"""
import argparse, json, os, sys
from collections import Counter
import psycopg

TABLE_FOR_ROUTE = {"a": "t_a", "b": "t_b", "rare": "t_rare"}


def load_ledger(path):
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--tables", default="t_a,t_b,t_rare")
    ap.add_argument("--out", required=True)
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--expect", default="no-loss", choices=["no-loss", "loss"])
    ap.add_argument("--unique-required", action="store_true",
                    help="duplicates are fatal (the sink mode promises uniqueness)")
    args = ap.parse_args()

    ledger = load_ledger(args.ledger)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    expected = {t: set() for t in tables}
    for row in ledger:
        t = TABLE_FOR_ROUTE.get(row["route"])
        if t in expected:
            expected[t].add(row["event_id"])

    outage_ids = {r["event_id"] for r in ledger if r["phase"] == "during-outage"}

    result = {"scenario": args.scenario, "tables": {}, "outage": {}, "pass": True}
    all_actual = set()

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT event_id FROM {t}")
            ids = [r[0] for r in cur.fetchall()]
            counts = Counter(ids)
            actual = set(ids)
            all_actual |= actual
            missing = sorted(expected[t] - actual)
            extra = sorted(actual - expected[t])
            dups = sorted(i for i, c in counts.items() if c > 1)
            result["tables"][t] = {
                "expected": len(expected[t]),
                "actual_distinct": len(actual),
                "rows": len(ids),
                "missing_count": len(missing),
                "missing_sample": missing[:20],
                "extra_count": len(extra),
                "extra_sample": extra[:20],
                "duplicate_count": len(dups),
                "duplicate_sample": dups[:20],
            }

    saved_from_outage = len(outage_ids & all_actual)
    result["outage"] = {
        "produced": len(outage_ids),
        "saved": saved_from_outage,
        "lost": len(outage_ids) - saved_from_outage,
    }

    lost_anywhere = any(v["missing_count"] > 0 for v in result["tables"].values())
    extra_anywhere = any(v["extra_count"] > 0 for v in result["tables"].values())
    dup_anywhere = any(v["duplicate_count"] > 0 for v in result["tables"].values())

    if args.expect == "no-loss":
        result["pass"] = not lost_anywhere and not extra_anywhere
        if args.unique_required and dup_anywhere:
            result["pass"] = False
    else:
        # A "loss" scenario only passes if it actually reproduced loss - a lab that
        # cannot reproduce the bug proves nothing about the fix.
        result["pass"] = lost_anywhere

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)

    print(json.dumps(result, indent=2))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
