# Experiment 3: Data Format Impact on Energy and Latency

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure how data format (CSV vs Parquet, with various compression codecs) affects energy and latency on both Maximus and Sirius, compared to CPU baseline.

**Architecture:** No engine code changes needed. Generate TPC-H data in multiple formats (CSV, Parquet-uncompressed, Parquet-Snappy, Parquet-ZSTD, Parquet-LZ4), then run existing benchmarks pointing at different data directories. Maximus auto-detects format from file extension. Sirius loads via DuckDB `read_parquet()` / `read_csv_auto()`.

**Tech Stack:** PyArrow (data generation), existing maxbench + Sirius DuckDB CLI, nvidia-smi (energy), Python (orchestration)

**Key Question:** Does columnar + compressed format (Parquet) reduce the total I/O + parse + transfer time enough to measurably reduce energy compared to CSV? And how does this compare across GPU (Maximus/Sirius) vs CPU (DuckDB)?

---

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `scripts/generate_parquet_data.py` | Create | Generate TPC-H Parquet data with various compression codecs |
| `scripts/run_format_experiment.py` | Create | Maximus format sweep: CSV vs Parquet variants |
| `scripts/run_format_experiment_sirius.py` | Create | Sirius format sweep: CSV vs Parquet variants |
| `scripts/setup_sirius_parquet.py` | Create | Create Sirius DuckDB databases from Parquet data |

---

### Task 1: Generate TPC-H data in all formats

**Files:**
- Create: `/home/xzw/gpu_db/scripts/generate_parquet_data.py`

- [ ] **Step 1: Write the data generation script**

```python
#!/usr/bin/env python3
"""
Generate TPC-H data in multiple Parquet formats with different compression codecs.
Reuses existing CSV data as source, converts to Parquet variants.

Output directory structure:
  tests/tpch/parquet-{codec}-{sf}/
    customer.parquet, lineitem.parquet, nation.parquet, orders.parquet,
    part.parquet, partsupp.parquet, region.parquet, supplier.parquet

Codecs: none, snappy, zstd, lz4
Scale factors: 1, 2, 5, 10
"""
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import sys
from pathlib import Path

MAXIMUS_DIR = Path("/home/xzw/Maximus")
TPCH_BASE = MAXIMUS_DIR / "tests" / "tpch"

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
CODECS = ["none", "snappy", "zstd", "lz4"]
SCALE_FACTORS = [1, 2, 5, 10]


def convert_sf(sf, codec):
    csv_dir = TPCH_BASE / f"csv-{sf}"
    if not csv_dir.exists():
        print(f"  [SKIP] CSV source not found: {csv_dir}")
        return

    out_dir = TPCH_BASE / f"parquet-{codec}-{sf}"
    out_dir.mkdir(parents=True, exist_ok=True)

    compression = codec if codec != "none" else None

    for table in TABLES:
        csv_file = csv_dir / f"{table}.csv"
        parquet_file = out_dir / f"{table}.parquet"

        if parquet_file.exists():
            print(f"  [EXISTS] {parquet_file.name}")
            continue

        print(f"  Converting {table}.csv -> {table}.parquet ({codec})...", end=" ", flush=True)
        arrow_table = pcsv.read_csv(str(csv_file))
        pq.write_table(arrow_table, str(parquet_file), compression=compression)

        csv_size = csv_file.stat().st_size / (1024 * 1024)
        pq_size = parquet_file.stat().st_size / (1024 * 1024)
        ratio = csv_size / pq_size if pq_size > 0 else 0
        print(f"{csv_size:.1f}MB -> {pq_size:.1f}MB ({ratio:.1f}x)")


def main():
    print("TPC-H Data Format Generator")
    print("=" * 60)

    for sf in SCALE_FACTORS:
        for codec in CODECS:
            print(f"\nSF={sf}, Codec={codec}")
            convert_sf(sf, codec)

    # Print size summary
    print(f"\n{'='*60}")
    print("SIZE SUMMARY (MB)")
    print(f"{'Format':<20} {'SF1':>8} {'SF2':>8} {'SF5':>8} {'SF10':>8}")
    print("-" * 52)

    for fmt in [f"csv"] + [f"parquet-{c}" for c in CODECS]:
        sizes = []
        for sf in SCALE_FACTORS:
            d = TPCH_BASE / f"{fmt}-{sf}"
            if d.exists():
                total = sum(f.stat().st_size for f in d.iterdir() if f.is_file())
                sizes.append(f"{total / (1024**2):.0f}")
            else:
                sizes.append("N/A")
        print(f"{fmt:<20} {sizes[0]:>8} {sizes[1]:>8} {sizes[2]:>8} {sizes[3]:>8}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the generation script**

```bash
python /home/xzw/gpu_db/scripts/generate_parquet_data.py
```

Expected: Creates `parquet-none-{sf}`, `parquet-snappy-{sf}`, `parquet-zstd-{sf}`, `parquet-lz4-{sf}` directories for each scale factor. Print size comparison table.

- [ ] **Step 3: Verify Maximus can read the generated Parquet files**

```bash
cd /home/xzw/Maximus && source .venv/bin/activate
LD_LIBRARY_PATH=".venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64:.venv/lib/python3.12/site-packages/libkvikio/lib64" \
  ./build/benchmarks/maxbench --benchmark tpch -q q1 -d gpu -r 3 \
  --path tests/tpch/parquet-snappy-1 -s gpu --engines maximus
```

Expected: Query runs and produces valid output.

- [ ] **Step 4: Commit**

```bash
cd /home/xzw/gpu_db
git add scripts/generate_parquet_data.py
git commit -m "feat: add TPC-H Parquet data generator with multiple compression codecs"
```

Note: Don't commit the generated data files (they're large). Add to .gitignore if needed.

---

### Task 2: Maximus format experiment

**Files:**
- Create: `/home/xzw/gpu_db/scripts/run_format_experiment.py`

- [ ] **Step 1: Write the Maximus format experiment script**

```python
#!/usr/bin/env python3
"""
Experiment 3a: Maximus CSV vs Parquet format comparison.

Matrix:
  - Formats: csv, parquet-none, parquet-snappy, parquet-zstd, parquet-lz4
  - Storage: gpu, cpu  (gpu=data preloaded to VRAM, cpu=transfer per query)
  - Scale factors: 1, 2 (SF10 OOMs on gpu storage)
  - Queries: q1, q6, q3, q9, q12
  - Reps: 20 (report min)

Measures: query latency (min_ms, avg_ms)
"""
from __future__ import annotations
import csv, os, re, subprocess, sys, time
from pathlib import Path

MAXIMUS_DIR = Path("/home/xzw/Maximus")
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
RESULTS_DIR = Path("/home/xzw/gpu_db/results/format_experiment")
LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]

FORMATS = {
    "csv":             lambda sf: MAXIMUS_DIR / "tests" / "tpch" / f"csv-{sf}",
    "parquet-none":    lambda sf: MAXIMUS_DIR / "tests" / "tpch" / f"parquet-none-{sf}",
    "parquet-snappy":  lambda sf: MAXIMUS_DIR / "tests" / "tpch" / f"parquet-snappy-{sf}",
    "parquet-zstd":    lambda sf: MAXIMUS_DIR / "tests" / "tpch" / f"parquet-zstd-{sf}",
    "parquet-lz4":     lambda sf: MAXIMUS_DIR / "tests" / "tpch" / f"parquet-lz4-{sf}",
}
STORAGE_MODES = ["gpu", "cpu"]
SCALE_FACTORS = [1, 2]
QUERIES = ["q1", "q6", "q3", "q9", "q12"]
N_REPS = 20


def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


def parse_timings(output):
    result = {}
    current = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)", line.strip())
        if qm: current = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current:
            ts = tm.group(1).strip().rstrip(",")
            result[current] = [int(t) for t in ts.split(",") if t.strip()]
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                q = parts[2]
                if q not in result:
                    result[q] = [int(t) for t in parts[3:] if t.strip()]
    return result


def parse_load_time(output):
    """Extract data loading time from maxbench output."""
    m = re.search(r"Loading times over repetitions \[ms\]:\s*(.*)", output)
    if m:
        ts = m.group(1).strip().rstrip(",")
        times = [int(t) for t in ts.split(",") if t.strip()]
        return min(times) if times else -1
    return -1


def run(sf, storage, fmt_name):
    data_path = FORMATS[fmt_name](sf)
    if not data_path.exists():
        return -1, {q: (-1, -1, "no_data") for q in QUERIES}

    cmd = [
        str(MAXBENCH), "--benchmark", "tpch",
        "-q", ",".join(QUERIES), "-d", "gpu",
        "-r", str(N_REPS), "--n_reps_storage", "1",
        "--path", str(data_path), "-s", storage,
        "--engines", "maximus",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=600, env=get_env())
        output = proc.stdout + (proc.stderr or "")
        timings = parse_timings(output)
        load_time = parse_load_time(output)
    except Exception as e:
        return -1, {q: (-1, -1, str(e)) for q in QUERIES}

    results = {}
    for q in QUERIES:
        if q in timings and timings[q]:
            t = timings[q]
            results[q] = (min(t), round(sum(t)/len(t), 1), "ok")
        else:
            results[q] = (-1, -1, "missing")
    return load_time, results


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    configs = [(sf, s, f) for sf in SCALE_FACTORS for s in STORAGE_MODES for f in FORMATS]
    total = len(configs)

    for i, (sf, storage, fmt) in enumerate(configs):
        print(f"\n[{i+1}/{total}] sf={sf} storage={storage} format={fmt}")
        t0 = time.perf_counter()
        load_ms, results = run(sf, storage, fmt)
        wall = time.perf_counter() - t0

        for q, (mn, avg, st) in results.items():
            rows.append({
                "sf": sf, "storage": storage, "format": fmt,
                "query": q, "load_ms": load_ms,
                "min_ms": mn, "avg_ms": avg, "status": st,
            })
            print(f"  {q}: min={mn}ms avg={avg}ms [{st}]")
        print(f"  load={load_ms}ms wall={wall:.1f}s")

    out = RESULTS_DIR / "maximus_format_summary.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "sf", "storage", "format", "query", "load_ms",
            "min_ms", "avg_ms", "status",
        ])
        w.writeheader()
        w.writerows(rows)

    print(f"\n{'='*60}")
    print(f"Results: {out}")

    # Summary table
    print(f"\n{'='*70}")
    print(f"SUMMARY (min_ms) - SF1, GPU storage")
    print(f"{'Format':<20} {'load':>6} {'q1':>6} {'q6':>6} {'q3':>6} {'q9':>6} {'q12':>6}")
    print("-" * 68)
    for fmt in FORMATS:
        matching = [r for r in rows if r["sf"] == 1 and r["storage"] == "gpu" and r["format"] == fmt]
        if not matching: continue
        load = matching[0]["load_ms"]
        vals = {r["query"]: r["min_ms"] for r in matching}
        print(f"{fmt:<20} {load:>6} {vals.get('q1','?'):>6} {vals.get('q6','?'):>6} "
              f"{vals.get('q3','?'):>6} {vals.get('q9','?'):>6} {vals.get('q12','?'):>6}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the experiment**

```bash
python /home/xzw/gpu_db/scripts/run_format_experiment.py
```

- [ ] **Step 3: Commit**

```bash
git add scripts/run_format_experiment.py results/format_experiment/
git commit -m "data: Maximus format experiment results (CSV vs Parquet variants)"
```

---

### Task 3: Sirius format experiment

**Files:**
- Create: `/home/xzw/gpu_db/scripts/setup_sirius_parquet.py`
- Create: `/home/xzw/gpu_db/scripts/run_format_experiment_sirius.py`

- [ ] **Step 1: Write the Sirius database setup script**

Sirius/DuckDB requires data to be imported into a DuckDB database first. Create a script that:
1. For each format (CSV, Parquet variants) and scale factor
2. Creates a DuckDB database
3. Imports all TPC-H tables using `read_csv_auto()` or `read_parquet()`

```python
#!/usr/bin/env python3
"""
Create DuckDB databases for Sirius benchmarks from different source formats.

For each (format, sf) pair, creates:
  /home/xzw/gpu_db/tests/tpch/sirius-{format}-{sf}/tpch.duckdb

Approach: DuckDB loads data into its native format regardless of source.
The experiment here measures the END-TO-END path including DuckDB's internal
handling of different source formats.

Note: Since DuckDB converts everything to its internal format at load time,
the source format only affects load time, not query time. For Sirius GPU
experiments, what matters is how data flows from DuckDB tables to GPU.
To test Parquet's impact on the GPU path specifically, we use Sirius's
native Parquet scan (gpu_parquet_scan) when available.
"""
import subprocess
from pathlib import Path

SIRIUS_DIR = Path("/home/xzw/sirius")
DUCKDB = SIRIUS_DIR / "build" / "release" / "duckdb"
MAXIMUS_DIR = Path("/home/xzw/Maximus")
OUT_BASE = Path("/home/xzw/gpu_db/tests/tpch")

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
FORMATS_AND_PATHS = {
    "csv": ("csv-{sf}", "read_csv_auto('{path}')"),
    "parquet-none": ("parquet-none-{sf}", "read_parquet('{path}')"),
    "parquet-snappy": ("parquet-snappy-{sf}", "read_parquet('{path}')"),
    "parquet-zstd": ("parquet-zstd-{sf}", "read_parquet('{path}')"),
    "parquet-lz4": ("parquet-lz4-{sf}", "read_parquet('{path}')"),
}
SCALE_FACTORS = [1, 2]


def setup_db(fmt, sf):
    dir_pattern, read_fn_template = FORMATS_AND_PATHS[fmt]
    src_dir = MAXIMUS_DIR / "tests" / "tpch" / dir_pattern.format(sf=sf)
    if not src_dir.exists():
        print(f"  [SKIP] Source not found: {src_dir}")
        return

    db_dir = OUT_BASE / f"sirius-{fmt}-{sf}"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "tpch.duckdb"

    if db_path.exists():
        print(f"  [EXISTS] {db_path}")
        return

    ext = "parquet" if "parquet" in fmt else "csv"
    sql_lines = []
    for table in TABLES:
        file_path = src_dir / f"{table}.{ext}"
        read_fn = read_fn_template.format(path=str(file_path))
        sql_lines.append(f"CREATE TABLE {table} AS SELECT * FROM {read_fn};")

    sql = "\n".join(sql_lines)
    print(f"  Creating {db_path}...")

    proc = subprocess.run(
        [str(DUCKDB), str(db_path)],
        input=sql, capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        print(f"  [ERROR] {proc.stderr[:200]}")
    else:
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"  Done ({size_mb:.1f} MB)")


def main():
    for sf in SCALE_FACTORS:
        for fmt in FORMATS_AND_PATHS:
            print(f"SF={sf} Format={fmt}")
            setup_db(fmt, sf)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write the Sirius benchmark runner**

```python
#!/usr/bin/env python3
"""
Experiment 3b: Sirius CSV vs Parquet format comparison.

Runs TPC-H queries via Sirius (DuckDB GPU extension) on databases
created from different source formats.

Note: DuckDB normalizes data at import time, so query latency should be
identical across source formats. The key measurement is:
1. Load time (format → DuckDB internal)
2. GPU query latency (should be ~same for all formats)
3. Energy (should be ~same for query, different for load)
"""
from __future__ import annotations
import csv, os, re, subprocess, sys, time
from pathlib import Path

SIRIUS_DIR = Path("/home/xzw/sirius")
DUCKDB = SIRIUS_DIR / "build" / "release" / "duckdb"
RESULTS_DIR = Path("/home/xzw/gpu_db/results/format_experiment")
DB_BASE = Path("/home/xzw/gpu_db/tests/tpch")

LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/rapids_logger/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/librmm/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libcudf/lib64",
]

FORMATS = ["csv", "parquet-none", "parquet-snappy", "parquet-zstd", "parquet-lz4"]
SCALE_FACTORS = [1, 2]
N_REPS = 20

# TPC-H queries for Sirius (simplified set matching Maximus experiment)
TPCH_QUERIES = {
    "q1": "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;",
    "q6": "SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24;",
    "q3": "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;",
    "q12": "SELECT l_shipmode, sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count, sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;",
}


def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


def get_vram_mb():
    """Get GPU VRAM in MB."""
    out = subprocess.check_output(
        ["nvidia-smi", "-i", "1", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
        text=True
    ).strip()
    return int(out)


def buffer_init_sql(vram_mb):
    primary_gb = int(vram_mb * 0.70 / 1024)
    secondary_gb = int(vram_mb * 0.35 / 1024)
    return f'call gpu_buffer_init("{primary_gb} GB", "{secondary_gb} GB");'


def run_sirius(sf, fmt, queries):
    db_path = DB_BASE / f"sirius-{fmt}-{sf}" / "tpch.duckdb"
    if not db_path.exists():
        return {q: (-1, -1, "no_db") for q in queries}

    vram = get_vram_mb()
    buf_sql = buffer_init_sql(vram)

    results = {}
    for qname, qsql in queries.items():
        # Build SQL: init buffer, then run query N_REPS times with timing
        sql = f".timer on\n{buf_sql}\n"
        for _ in range(N_REPS):
            sql += f'call gpu_processing("{qsql}");\n'

        try:
            proc = subprocess.run(
                [str(DUCKDB), str(db_path)],
                input=sql, capture_output=True, text=True,
                timeout=300, env=get_env(),
            )
            output = proc.stdout + (proc.stderr or "")
            # Parse DuckDB timer output: "Run Time (s): real X.XXX user X.XXX sys X.XXX"
            times = re.findall(r"Run Time \(s\): real (\d+\.\d+)", output)
            if times:
                # Skip first timing (buffer init), convert to ms
                query_times = [float(t) * 1000 for t in times[1:]]
                if query_times:
                    results[qname] = (round(min(query_times), 1),
                                      round(sum(query_times)/len(query_times), 1), "ok")
                else:
                    results[qname] = (-1, -1, "no_timing")
            else:
                results[qname] = (-1, -1, "parse_error")
        except Exception as e:
            results[qname] = (-1, -1, str(e)[:50])

    return results


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    configs = [(sf, fmt) for sf in SCALE_FACTORS for fmt in FORMATS]
    for i, (sf, fmt) in enumerate(configs):
        print(f"\n[{i+1}/{len(configs)}] Sirius sf={sf} format={fmt}")
        t0 = time.perf_counter()
        results = run_sirius(sf, fmt, TPCH_QUERIES)
        wall = time.perf_counter() - t0

        for q, (mn, avg, st) in results.items():
            rows.append({
                "engine": "sirius", "sf": sf, "format": fmt,
                "query": q, "min_ms": mn, "avg_ms": avg, "status": st,
            })
            print(f"  {q}: min={mn}ms avg={avg}ms [{st}]")
        print(f"  wall={wall:.1f}s")

    out = RESULTS_DIR / "sirius_format_summary.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "engine", "sf", "format", "query", "min_ms", "avg_ms", "status",
        ])
        w.writeheader()
        w.writerows(rows)
    print(f"\nResults: {out}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run setup and experiments**

```bash
# 1. Generate Parquet data (Task 1)
python scripts/generate_parquet_data.py

# 2. Create Sirius databases
python scripts/setup_sirius_parquet.py

# 3. Run Sirius experiment
python scripts/run_format_experiment_sirius.py
```

- [ ] **Step 4: Commit**

```bash
git add scripts/setup_sirius_parquet.py scripts/run_format_experiment_sirius.py
git commit -m "data: Sirius format experiment results"
```

---

### Task 4: CPU baseline comparison

**Files:**
- Create: `/home/xzw/gpu_db/scripts/run_format_experiment_cpu.py`

- [ ] **Step 1: Write CPU baseline script**

Run the same TPC-H queries on CPU (DuckDB without GPU) to establish the CPU energy/latency baseline that we're comparing against.

```python
#!/usr/bin/env python3
"""
Experiment 3c: CPU baseline - DuckDB without GPU on same formats.
Establishes the reference point for GPU vs CPU energy comparison.
"""
from __future__ import annotations
import csv, os, re, subprocess, time
from pathlib import Path

DUCKDB = Path("/home/xzw/sirius/build/release/duckdb")
MAXIMUS_DIR = Path("/home/xzw/Maximus")
RESULTS_DIR = Path("/home/xzw/gpu_db/results/format_experiment")

FORMATS = {
    "csv": ("csv-{sf}", "csv", "read_csv_auto('{path}')"),
    "parquet-none": ("parquet-none-{sf}", "parquet", "read_parquet('{path}')"),
    "parquet-snappy": ("parquet-snappy-{sf}", "parquet", "read_parquet('{path}')"),
    "parquet-zstd": ("parquet-zstd-{sf}", "parquet", "read_parquet('{path}')"),
    "parquet-lz4": ("parquet-lz4-{sf}", "parquet", "read_parquet('{path}')"),
}
TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
SCALE_FACTORS = [1, 2]
N_REPS = 10

QUERIES = {
    "q1": "SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_extendedprice*(1-l_discount)*(1+l_tax)), avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(*) FROM lineitem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;",
    "q6": "SELECT sum(l_extendedprice*l_discount) FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24;",
    "q3": "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey AND l_orderkey=o_orderkey AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;",
    "q12": "SELECT l_shipmode, sum(CASE WHEN o_orderpriority='1-URGENT' OR o_orderpriority='2-HIGH' THEN 1 ELSE 0 END), sum(CASE WHEN o_orderpriority<>'1-URGENT' AND o_orderpriority<>'2-HIGH' THEN 1 ELSE 0 END) FROM orders, lineitem WHERE o_orderkey=l_orderkey AND l_shipmode IN ('MAIL','SHIP') AND l_commitdate<l_receiptdate AND l_shipdate<l_commitdate AND l_receiptdate>='1994-01-01' AND l_receiptdate<'1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;",
}


def run_cpu(sf, fmt):
    dir_pattern, ext, read_fn_template = FORMATS[fmt]
    src_dir = MAXIMUS_DIR / "tests" / "tpch" / dir_pattern.format(sf=sf)
    if not src_dir.exists():
        return {q: (-1, -1, "no_data") for q in QUERIES}

    # Create tables from source format, then query
    create_sql = ""
    for table in TABLES:
        path = src_dir / f"{table}.{ext}"
        read_fn = read_fn_template.format(path=str(path))
        create_sql += f"CREATE TABLE {table} AS SELECT * FROM {read_fn};\n"

    results = {}
    for qname, qsql in QUERIES.items():
        sql = f".timer on\n{create_sql}\n"
        for _ in range(N_REPS):
            sql += f"{qsql}\n"

        try:
            proc = subprocess.run(
                [str(DUCKDB)], input=sql,
                capture_output=True, text=True, timeout=300,
            )
            output = proc.stdout + (proc.stderr or "")
            times = re.findall(r"Run Time \(s\): real (\d+\.\d+)", output)
            if len(times) > len(TABLES):
                query_times = [float(t) * 1000 for t in times[len(TABLES):]]
                results[qname] = (round(min(query_times), 1),
                                  round(sum(query_times)/len(query_times), 1), "ok")
            else:
                results[qname] = (-1, -1, "no_timing")
        except Exception as e:
            results[qname] = (-1, -1, str(e)[:50])

    return results


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    for sf in SCALE_FACTORS:
        for fmt in FORMATS:
            print(f"CPU DuckDB sf={sf} format={fmt}")
            results = run_cpu(sf, fmt)
            for q, (mn, avg, st) in results.items():
                rows.append({
                    "engine": "duckdb_cpu", "sf": sf, "format": fmt,
                    "query": q, "min_ms": mn, "avg_ms": avg, "status": st,
                })
                print(f"  {q}: min={mn}ms avg={avg}ms [{st}]")

    out = RESULTS_DIR / "cpu_format_summary.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["engine","sf","format","query","min_ms","avg_ms","status"])
        w.writeheader()
        w.writerows(rows)
    print(f"\nResults: {out}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run CPU baseline**

```bash
python scripts/run_format_experiment_cpu.py
```

- [ ] **Step 3: Commit**

---

### Task 5: Energy measurement for all format experiments

**Files:**
- Create: `/home/xzw/gpu_db/scripts/run_format_energy.py`

- [ ] **Step 1: Write unified energy measurement script**

Add nvidia-smi 50ms sampling to all three experiment variants (Maximus GPU, Sirius GPU, DuckDB CPU). Follow the methodology from `run_maximus_metrics.py`:
1. Start nvidia-smi background sampling
2. Run benchmark
3. Parse power samples, detect steady state
4. Calculate energy = avg_power_steady × latency

- [ ] **Step 2: Run energy experiments**

- [ ] **Step 3: Commit results**

---

### Task 6: Combined analysis and visualization

**Files:**
- Create: `/home/xzw/gpu_db/scripts/plot_format_results.py`

- [ ] **Step 1: Write analysis script**

Create a script that:
1. Reads all three CSV result files (Maximus, Sirius, CPU)
2. Computes speedup ratios (Parquet vs CSV for each engine)
3. Computes energy ratios
4. Generates comparison plots:
   - Latency by format × engine (grouped bar chart)
   - Energy by format × engine
   - Load time by format (stacked: I/O + parse + transfer)

- [ ] **Step 2: Generate plots**

- [ ] **Step 3: Commit**

---

## Expected Results

### Data Size Predictions

| Format | SF1 Size | Compression Ratio vs CSV |
|--------|----------|-------------------------|
| CSV | ~1.1 GB | 1x (baseline) |
| Parquet (none) | ~300-400 MB | ~3x (columnar encoding alone) |
| Parquet (Snappy) | ~200-250 MB | ~5x |
| Parquet (ZSTD) | ~150-200 MB | ~6-7x |
| Parquet (LZ4) | ~220-270 MB | ~4-5x |

### Latency Predictions

**GPU-resident (storage=gpu):**
- CSV load time >> Parquet load time (CSV parsing is expensive)
- Query latency: **same** (data is in GPU memory in columnar format regardless of source)
- Total time: Parquet wins due to faster loading

**CPU-storage (storage=cpu):**
- CSV: large transfer over PCIe
- Parquet: smaller file → faster read, but cuDF still needs to decompress
- Net: Parquet likely faster, especially compressed variants

**CPU baseline (DuckDB):**
- DuckDB optimized for Parquet (native reader, column pruning, predicate pushdown)
- CSV requires full parsing → slower
- Parquet-ZSTD likely fastest due to smallest I/O

### Key Insight

The format experiment will show that **data format primarily affects I/O and loading time**, not query execution time. Once data is in GPU memory, the format doesn't matter. But for energy, the total time (load + query) determines the total energy consumption, so format matters for the complete picture.
