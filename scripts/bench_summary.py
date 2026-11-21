"""Summarise scripts/bench.sh progress files into docs/results/bench_throughput.json."""
import json
from datetime import datetime
from pathlib import Path

RUN = Path("/data/runs/bench")
man = json.loads((RUN / "manifest.json").read_text())
out = {"producer": {k: man[k] for k in ("cells", "sim_minutes", "wall_seconds", "events_per_wall_second", "delivery")},
       "runs": []}
for label, queries in (("bronze_100k", "bronze"), ("silver_100k", "silver"), ("silver_500k", "silver"),
                       ("both_100k", "bronze,silver")):
    pr = json.loads((RUN / f"progress_bench_{label}.json").read_text())["queries"]
    rows = [r for q in pr.values() for r in q if r["rows"] > 0]
    starts = [datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")).timestamp() for r in rows]
    ends = [s + r["trigger_ms"] / 1000 for s, r in zip(starts, rows)]
    events = max(sum(r["rows"] for r in q) for q in pr.values())   # each query reads the whole topic
    wall = max(ends) - min(starts)
    out["runs"].append({
        "label": f"{queries.replace(',', ' + ')} · maxOffsetsPerTrigger {label.split('_')[1]}",
        "queries": queries, "events": events, "batches": len(rows), "seconds": round(wall, 2),
        "rows_per_s": round(events / wall, 0),
        "max_state_rows": max((r["state_rows"] for r in rows), default=0),
        "max_batch_ms": max(r["trigger_ms"] for r in rows)})
Path("docs/results/bench_throughput.json").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=1))
