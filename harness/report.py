#!/usr/bin/env python3
"""Assemble RESULTS.md from everything a run left on disk.

Nothing here is written by hand: the case definition comes from its .env file, the
initial data from the producer's ledger, the evidence from the logs the run captured,
and the numbers from verdict.json. If a scenario has not been run, it says so instead
of guessing.
"""
import json, os, sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "harness", "out")
SCEN = os.path.join(ROOT, "harness", "scenarios")

ORDER = ["S01", "S02", "S02K", "S02L", "S03", "S04", "S05", "S06", "S07", "S08", "S09",
         "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17", "S18",
         "S19", "S19H", "S20", "S21", "S21N", "S22", "S23", "S24"]


def read(path, limit=None):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8", errors="replace") as fh:
        data = fh.read()
    if limit and len(data) > limit:
        data = data[:limit] + "\n... (truncated)\n"
    return data


def env_of(s):
    return read(os.path.join(SCEN, f"{s}.env"))


def ledger_of(s):
    p = os.path.join(OUT, s, "produced.jsonl")
    if not os.path.exists(p):
        return None
    phases = defaultdict(list)
    routes = Counter()
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            phases[r["phase"]].append(r["event_id"])
            routes[r["route"]] += 1
    return phases, routes


def verdict_of(s):
    p = os.path.join(OUT, s, "verdict.json")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def env_value(text, key):
    if not text:
        return ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(key + "="):
            return line.split("=", 1)[1].strip().strip('"')
    return ""


def main():
    rows = []
    bodies = []

    for s in ORDER:
        env = env_of(s)
        if env is None:
            continue
        v = verdict_of(s)
        led = ledger_of(s)
        desc = env_value(env, "DESCRIPTION")
        expect = env_value(env, "EXPECT") or "no-loss"

        if v is None:
            rows.append(f"| {s} | — | — | — | — | {expect} | **not run** | — |")
            bodies.append(f"## {s} — not run yet\n\n{desc}\n")
            continue

        o = v["outage"]
        miss = sum(t["missing_count"] for t in v["tables"].values())
        dup = sum(t["duplicate_count"] for t in v["tables"].values())
        extra = sum(t["extra_count"] for t in v["tables"].values())
        verdict = "PASS" if v["pass"] else "**FAIL**"
        rows.append(
            f"| {s} | {o['produced']} | **{o['saved']}** | {o['lost']} | {miss} | {dup} "
            f"| {expect} | {verdict} |")

        b = [f"## {s}\n", f"{desc}\n"]

        b.append("### Case definition (`harness/scenarios/%s.env`, verbatim)\n" % s)
        b.append("```ini\n" + env.strip() + "\n```\n")

        if led:
            phases, routes = led
            b.append("### Initial data produced\n")
            b.append("| phase | events | event_id range |")
            b.append("|---|---|---|")
            for ph in ("before-outage", "during-outage", "after-restart"):
                ids = phases.get(ph, [])
                if ids:
                    b.append(f"| {ph} | {len(ids)} | {min(ids)}–{max(ids)} |")
            b.append("")
            b.append("Route split: " + ", ".join(f"`{k}`={n}" for k, n in sorted(routes.items())) + "\n")

        off = read(os.path.join(OUT, s, "starting-offsets.log"), 2500)
        if off and off.strip():
            b.append("### Where the restarted source actually began\n")
            b.append("```\n" + off.strip() + "\n```\n")

        cg = read(os.path.join(OUT, s, "consumer-group.log"), 1500)
        if cg and cg.strip():
            b.append("### Consumer group after the run\n")
            b.append("```\n" + cg.strip() + "\n```\n")

        b.append("### Output — per destination table\n")
        b.append("| table | expected | distinct saved | rows | missing | duplicates | extra |")
        b.append("|---|---|---|---|---|---|---|")
        for t, d in sorted(v["tables"].items()):
            b.append(f"| `{t}` | {d['expected']} | {d['actual_distinct']} | {d['rows']} "
                     f"| {d['missing_count']} | {d['duplicate_count']} | {d['extra_count']} |")
        b.append("")
        b.append(f"**Outage window: {o['produced']} produced, {o['saved']} saved, "
                 f"{o['lost']} lost.**\n")
        if miss:
            sample = []
            for t, d in sorted(v["tables"].items()):
                if d["missing_sample"]:
                    sample.append(f"`{t}`: {d['missing_sample'][:10]}")
            if sample:
                b.append("First missing event ids — " + "; ".join(sample) + "\n")

        b.append("### Raw verdict\n")
        b.append("```json\n" + json.dumps(v, indent=2) + "\n```\n")
        bodies.append("\n".join(b))

    header = [
        "# Results",
        "",
        "Generated by `harness/report.py` from what each run left on disk — the case",
        "definition from its `.env`, the initial data from the producer's ledger, the",
        "evidence from the captured logs, and the numbers from `verdict.json`. Nothing",
        "in this file is written by hand.",
        "",
        "`outage.produced` is how many events were produced **while the job was down**;",
        "`outage.saved` is how many of those are in the database afterwards. Those two",
        "columns are the whole question.",
        "",
        "A scenario whose `EXPECT` is `loss` passes only when it actually reproduces loss —",
        "a lab that cannot reproduce the bug proves nothing about the fix.",
        "",
        "| scenario | produced in outage | saved | lost | missing (all phases) | duplicates | expected | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    header += rows
    header += ["", "---", ""]

    md = "\n".join(header) + "\n" + "\n---\n\n".join(bodies) + "\n"
    md += """
## Not yet implemented

Specified in the design but not runnable yet, listed so the gap is visible rather than
implied:

- **S07** — committed-offset expiry (`offsets.retention.minutes=1`); needs a broker
  restart between scenarios.
- **S12 / S13** — fan-out resume from `max(offset)` vs a completion frontier; needs the
  custom `OffsetsInitializer` that reads `ingest_progress`.
- **S16** — XA exactly-once (`JdbcSink.exactlyOnceSink`, `max_prepared_transactions>0`,
  `withTransactionPerConnection(true)`).
- **S17 / S18 / S19 / S20** — the two-hop router shape; needs two jobs running together.
- **S21 / S22** — timestamp positioning, including the Flink 1.17 case where a failed
  time-index lookup starts a partition at its END offset and silently skips its backlog.
"""
    with open(os.path.join(ROOT, "RESULTS.md"), "w", encoding="utf-8") as fh:
        fh.write(md)
    print(f"wrote RESULTS.md ({len(md)} bytes, {len(rows)} scenarios)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
