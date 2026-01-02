"""
Storage Efficiency Test for TSDB Benchmark

Measures actual disk usage by checking filesystem inside containers
after flushing data to disk. Also monitors CPU and memory usage.
"""

import asyncio
import subprocess
import time
import json
import threading
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

import httpx
import psycopg

from data_generator import SourcefulDataGenerator

console = Console()


class ResourceMonitor:
    """Monitor CPU and memory usage of containers during ingestion."""

    def __init__(self, containers: list[str]):
        self.containers = containers
        self.samples = {c: [] for c in containers}
        self.running = False
        self._thread = None

    def start(self):
        """Start monitoring in background thread."""
        self.running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop monitoring and return results."""
        self.running = False
        if self._thread:
            self._thread.join(timeout=2)
        return self._compute_stats()

    def _monitor_loop(self):
        """Sample docker stats every second."""
        while self.running:
            try:
                result = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format",
                     "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split("\n"):
                        parts = line.split("\t")
                        if len(parts) >= 3:
                            name = parts[0]
                            if name in self.containers:
                                cpu = float(parts[1].rstrip("%"))
                                # Parse memory like "1.5GiB / 7.5GiB" or "500MiB / 7.5GiB"
                                mem_str = parts[2].split("/")[0].strip()
                                mem_mb = self._parse_memory(mem_str)
                                self.samples[name].append({"cpu": cpu, "mem_mb": mem_mb})
            except Exception:
                pass
            time.sleep(1)

    def _parse_memory(self, mem_str: str) -> float:
        """Parse memory string to MB."""
        mem_str = mem_str.strip()
        if "GiB" in mem_str:
            return float(mem_str.replace("GiB", "")) * 1024
        elif "MiB" in mem_str:
            return float(mem_str.replace("MiB", ""))
        elif "KiB" in mem_str:
            return float(mem_str.replace("KiB", "")) / 1024
        elif "GB" in mem_str:
            return float(mem_str.replace("GB", "")) * 1000
        elif "MB" in mem_str:
            return float(mem_str.replace("MB", ""))
        return 0

    def _compute_stats(self) -> dict:
        """Compute average and peak stats."""
        stats = {}
        for name, samples in self.samples.items():
            if samples:
                cpus = [s["cpu"] for s in samples]
                mems = [s["mem_mb"] for s in samples]
                stats[name] = {
                    "cpu_avg": sum(cpus) / len(cpus),
                    "cpu_max": max(cpus),
                    "mem_avg_mb": sum(mems) / len(mems),
                    "mem_max_mb": max(mems),
                    "samples": len(samples)
                }
            else:
                stats[name] = {"cpu_avg": 0, "cpu_max": 0, "mem_avg_mb": 0, "mem_max_mb": 0, "samples": 0}
        return stats

# Elasticsearch TSDB index template
ES_INDEX_TEMPLATE = {
    "settings": {
        "index": {
            "mode": "time_series",
            "routing_path": ["der_id"],
            "time_series": {
                "start_time": "2020-01-01T00:00:00Z",
                "end_time": "2030-01-01T00:00:00Z"
            },
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "codec": "best_compression"
        }
    },
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "der_id": {"type": "keyword", "time_series_dimension": True},
            "der_type": {"type": "keyword"},
            "site_id": {"type": "keyword"},
            "device_id": {"type": "keyword"},
            "wallet_id": {"type": "keyword"},
            "make": {"type": "keyword"},
            "W": {"type": "long", "time_series_metric": "gauge"}
        }
    }
}


def get_container_disk_usage(container_name: str, path: str) -> dict:
    """Get disk usage for a path inside a container."""
    try:
        # Get total size of directory
        result = subprocess.run(
            ["docker", "exec", container_name, "du", "-sb", path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            size_bytes = int(result.stdout.split()[0])
            return {"bytes": size_bytes, "mb": size_bytes / (1024 * 1024)}
    except Exception as e:
        console.print(f"[yellow]Warning: Could not get disk usage for {container_name}: {e}[/]")

    return {"bytes": 0, "mb": 0}


def flush_database(container_name: str, db_type: str) -> bool:
    """Force flush data to disk for each database type."""
    try:
        if db_type == "victoriametrics":
            # VictoriaMetrics: force flush via API
            subprocess.run(
                ["curl", "-s", "http://localhost:8428/internal/force_flush"],
                capture_output=True, timeout=30
            )
        elif db_type == "questdb":
            # QuestDB: checkpoint via SQL
            subprocess.run(
                ["curl", "-s", "http://localhost:9000/exec?query=CHECKPOINT"],
                capture_output=True, timeout=30
            )
        elif db_type == "clickhouse":
            # ClickHouse: SYSTEM FLUSH LOGS and optimize
            subprocess.run(
                ["docker", "exec", container_name, "clickhouse-client",
                 "--query", "SYSTEM FLUSH LOGS; OPTIMIZE TABLE energy FINAL"],
                capture_output=True, timeout=60
            )
        elif db_type == "timescaledb":
            # PostgreSQL: CHECKPOINT
            subprocess.run(
                ["docker", "exec", container_name, "psql", "-U", "postgres",
                 "-c", "CHECKPOINT;"],
                capture_output=True, timeout=30
            )
        elif db_type == "elasticsearch":
            # Elasticsearch: force flush and refresh
            subprocess.run(
                ["curl", "-s", "-X", "POST", "http://localhost:9200/energy/_flush?force=true"],
                capture_output=True, timeout=30
            )
            subprocess.run(
                ["curl", "-s", "-X", "POST", "http://localhost:9200/energy/_forcemerge?max_num_segments=1"],
                capture_output=True, timeout=120
            )
        elif db_type == "influxdb3":
            # InfluxDB3: no explicit flush needed, data is written to parquet
            time.sleep(2)  # Wait for background writes
        elif db_type == "influxdb2":
            # InfluxDB2: flush via API
            subprocess.run(
                ["curl", "-s", "-X", "POST", "http://localhost:8086/api/v2/flush",
                 "-H", "Authorization: Token benchmark-token-secret"],
                capture_output=True, timeout=30
            )
            time.sleep(2)
        return True
    except Exception as e:
        console.print(f"[yellow]Warning: Flush failed for {container_name}: {e}[/]")
        return False


async def ingest_data(rows: int = 1_000_000) -> dict:
    """Ingest data to all databases and return timing info."""

    # Calculate how many DERs and duration we need
    # 10,000 DERs * duration_seconds = rows
    duration_seconds = rows // 10_000

    generator = SourcefulDataGenerator(
        num_wallets=1000,
        sites_per_wallet=2,
        devices_per_site=1,
        ders_per_device=5
    )

    base_time = datetime.now() - timedelta(seconds=duration_seconds)

    console.print(f"\n[bold]Generating {rows:,} rows of test data...[/]")
    all_readings = list(generator.generate_batch(
        start_time=base_time,
        duration_seconds=duration_seconds,
        interval_seconds=1
    ))

    actual_rows = len(all_readings)
    console.print(f"[green]Generated {actual_rows:,} rows[/]")

    # Calculate raw data size (approximate)
    raw_size_bytes = actual_rows * 200  # ~200 bytes per row in JSON
    raw_size_mb = raw_size_bytes / (1024 * 1024)

    # Start resource monitoring
    containers = ["victoriametrics", "questdb", "clickhouse", "timescaledb", "elasticsearch", "influxdb3", "influxdb2"]
    monitor = ResourceMonitor(containers)
    monitor.start()
    console.print("[dim]Resource monitoring started...[/]")

    results = {}
    batch_size = 10000

    # VictoriaMetrics
    console.print("\n[cyan]▶ Ingesting to VictoriaMetrics...[/]")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            start = time.perf_counter()
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]
                lines = [generator.to_influx_line_protocol(r) for r in chunk]
                data = "\n".join(lines)
                await client.post(
                    "http://localhost:8428/write",
                    content=data,
                    headers={"Content-Type": "text/plain"}
                )
            elapsed = time.perf_counter() - start
            results["VictoriaMetrics"] = {"time": elapsed, "rows": actual_rows}
            console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["VictoriaMetrics"] = {"time": 0, "rows": 0, "error": str(e)}

    # QuestDB
    console.print("\n[cyan]▶ Ingesting to QuestDB...[/]")
    try:
        # Create table first
        async with httpx.AsyncClient(timeout=120.0) as client:
            await client.get("http://localhost:9000/exec", params={
                "query": """
                    CREATE TABLE IF NOT EXISTS energy (
                        timestamp TIMESTAMP,
                        der_id SYMBOL,
                        der_type SYMBOL,
                        site_id SYMBOL,
                        device_id SYMBOL,
                        wallet_id SYMBOL,
                        make SYMBOL,
                        W LONG
                    ) timestamp(timestamp) PARTITION BY DAY WAL;
                """
            })

        start = time.perf_counter()
        for i in range(0, len(all_readings), batch_size):
            chunk = all_readings[i:i + batch_size]
            lines = [generator.to_influx_line_protocol(r) for r in chunk]
            data = "\n".join(lines)

            reader, writer = await asyncio.open_connection("localhost", 9009)
            try:
                writer.write(data.encode() + b"\n")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        elapsed = time.perf_counter() - start
        results["QuestDB"] = {"time": elapsed, "rows": actual_rows}
        console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["QuestDB"] = {"time": 0, "rows": 0, "error": str(e)}

    # ClickHouse
    console.print("\n[cyan]▶ Ingesting to ClickHouse...[/]")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # Create table
            create_table = """
                CREATE TABLE IF NOT EXISTS energy (
                    timestamp DateTime64(3),
                    der_id String,
                    der_type LowCardinality(String),
                    site_id LowCardinality(String),
                    device_id LowCardinality(String),
                    wallet_id LowCardinality(String),
                    make LowCardinality(String),
                    W Int64
                ) ENGINE = MergeTree()
                ORDER BY (site_id, der_id, timestamp)
                PARTITION BY toYYYYMMDD(timestamp)
            """
            await client.post("http://localhost:8123", content=create_table)

            start = time.perf_counter()
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]

                import json
                rows = []
                for r in chunk:
                    ts_str = r.timestamp.rstrip("Z")
                    dt = datetime.fromisoformat(ts_str)
                    rows.append(json.dumps({
                        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                        "der_id": r.der_id,
                        "der_type": r.type,
                        "site_id": r.site_id,
                        "device_id": r.device_id,
                        "wallet_id": r.wallet_id,
                        "make": r.make,
                        "W": r.W
                    }))

                json_lines = "\n".join(rows)
                await client.post(
                    "http://localhost:8123/?query=INSERT INTO energy FORMAT JSONEachRow",
                    content=json_lines,
                    headers={"Content-Type": "application/json"}
                )

            elapsed = time.perf_counter() - start
            results["ClickHouse"] = {"time": elapsed, "rows": actual_rows}
            console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["ClickHouse"] = {"time": 0, "rows": 0, "error": str(e)}

    # TimescaleDB
    console.print("\n[cyan]▶ Ingesting to TimescaleDB...[/]")
    try:
        conn = await psycopg.AsyncConnection.connect(
            "postgresql://postgres:postgres@localhost:5432/sourceful"
        )

        async with conn.cursor() as cur:
            await cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS energy (
                    timestamp TIMESTAMPTZ NOT NULL,
                    der_id TEXT NOT NULL,
                    der_type TEXT NOT NULL,
                    site_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    wallet_id TEXT NOT NULL,
                    make TEXT,
                    W BIGINT
                );
            """)
            try:
                await cur.execute("""
                    SELECT create_hypertable('energy', 'timestamp', if_not_exists => TRUE);
                """)
            except:
                pass
        await conn.commit()

        start = time.perf_counter()
        async with conn.cursor() as cur:
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]
                tuples = []
                for r in chunk:
                    ts_str = r.timestamp.rstrip("Z")
                    dt = datetime.fromisoformat(ts_str)
                    tuples.append((dt, r.der_id, r.type, r.site_id,
                                   r.device_id, r.wallet_id, r.make, r.W))

                await cur.executemany("""
                    INSERT INTO energy (timestamp, der_id, der_type, site_id,
                                        device_id, wallet_id, make, W)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, tuples)
        await conn.commit()

        elapsed = time.perf_counter() - start
        results["TimescaleDB"] = {"time": elapsed, "rows": actual_rows}
        console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")

        await conn.close()
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["TimescaleDB"] = {"time": 0, "rows": 0, "error": str(e)}

    # Elasticsearch (TSDB mode)
    console.print("\n[cyan]▶ Ingesting to Elasticsearch (TSDB mode)...[/]")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # Create TSDB index
            await client.put(
                "http://localhost:9200/energy",
                json=ES_INDEX_TEMPLATE,
                headers={"Content-Type": "application/json"}
            )

            start = time.perf_counter()
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]

                # Build bulk request
                bulk_lines = []
                for r in chunk:
                    bulk_lines.append(json.dumps({"index": {}}))
                    bulk_lines.append(json.dumps({
                        "@timestamp": r.timestamp,
                        "der_id": r.der_id,
                        "der_type": r.type,
                        "site_id": r.site_id,
                        "device_id": r.device_id,
                        "wallet_id": r.wallet_id,
                        "make": r.make,
                        "W": r.W
                    }))

                bulk_data = "\n".join(bulk_lines) + "\n"
                await client.post(
                    "http://localhost:9200/energy/_bulk",
                    content=bulk_data,
                    headers={"Content-Type": "application/x-ndjson"}
                )

            elapsed = time.perf_counter() - start
            results["Elasticsearch"] = {"time": elapsed, "rows": actual_rows}
            console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["Elasticsearch"] = {"time": 0, "rows": 0, "error": str(e)}

    # InfluxDB3 Core
    console.print("\n[cyan]▶ Ingesting to InfluxDB3 Core...[/]")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            start = time.perf_counter()
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]
                lines = [generator.to_influx_line_protocol(r) for r in chunk]
                data = "\n".join(lines)
                # Use precision=ns since generator outputs nanoseconds
                resp = await client.post(
                    "http://localhost:8181/api/v3/write_lp?db=energy&precision=ns",
                    content=data,
                    headers={"Content-Type": "text/plain"}
                )
                if resp.status_code != 200 and resp.status_code != 204:
                    console.print(f"  [yellow]Warning: {resp.status_code} - {resp.text[:100]}[/]")
                    break

            elapsed = time.perf_counter() - start
            results["InfluxDB3"] = {"time": elapsed, "rows": actual_rows}
            console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["InfluxDB3"] = {"time": 0, "rows": 0, "error": str(e)}

    # InfluxDB2
    console.print("\n[cyan]▶ Ingesting to InfluxDB2...[/]")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            start = time.perf_counter()
            for i in range(0, len(all_readings), batch_size):
                chunk = all_readings[i:i + batch_size]
                lines = [generator.to_influx_line_protocol(r) for r in chunk]
                data = "\n".join(lines)
                await client.post(
                    "http://localhost:8086/api/v2/write?org=sourceful&bucket=energy&precision=ns",
                    content=data,
                    headers={
                        "Content-Type": "text/plain",
                        "Authorization": "Token benchmark-token-secret"
                    }
                )

            elapsed = time.perf_counter() - start
            results["InfluxDB2"] = {"time": elapsed, "rows": actual_rows}
            console.print(f"  ✓ {actual_rows:,} rows in {elapsed:.1f}s")
    except Exception as e:
        console.print(f"  [red]✗ Error: {e}[/]")
        results["InfluxDB2"] = {"time": 0, "rows": 0, "error": str(e)}

    # Stop resource monitoring
    resource_stats = monitor.stop()
    console.print("[dim]Resource monitoring stopped.[/]")

    return {
        "results": results,
        "raw_size_mb": raw_size_mb,
        "rows": actual_rows,
        "resources": resource_stats
    }


def measure_storage() -> dict:
    """Measure actual disk storage for each database."""

    databases = {
        "VictoriaMetrics": {
            "container": "victoriametrics",
            "path": "/storage",
            "type": "victoriametrics"
        },
        "QuestDB": {
            "container": "questdb",
            "path": "/var/lib/questdb/db",
            "type": "questdb"
        },
        "ClickHouse": {
            "container": "clickhouse",
            "path": "/var/lib/clickhouse",
            "type": "clickhouse"
        },
        "TimescaleDB": {
            "container": "timescaledb",
            "path": "/var/lib/postgresql/data",
            "type": "timescaledb"
        },
        "Elasticsearch": {
            "container": "elasticsearch",
            "path": "/usr/share/elasticsearch/data",
            "type": "elasticsearch"
        },
        "InfluxDB2": {
            "container": "influxdb2",
            "path": "/var/lib/influxdb2",
            "type": "influxdb2"
        },
        "InfluxDB3": {
            "container": "influxdb3",
            "path": "/var/lib/influxdb3",
            "type": "influxdb3"
        }
    }

    results = {}

    console.print("\n[bold]Flushing data to disk...[/]")
    for name, config in databases.items():
        flush_database(config["container"], config["type"])
        console.print(f"  ✓ {name} flushed")

    # Wait for flush to complete
    console.print("\n[dim]Waiting 10s for data to settle...[/]")
    time.sleep(10)

    console.print("\n[bold]Measuring disk usage...[/]")
    for name, config in databases.items():
        usage = get_container_disk_usage(config["container"], config["path"])
        results[name] = usage
        console.print(f"  {name}: {usage['mb']:.1f} MB")

    return results


def print_results(ingest_results: dict, storage_results: dict):
    """Print formatted results."""

    raw_size_mb = ingest_results["raw_size_mb"]
    rows = ingest_results["rows"]
    resources = ingest_results.get("resources", {})

    # Map container names to display names
    container_to_name = {
        "victoriametrics": "VictoriaMetrics",
        "questdb": "QuestDB",
        "clickhouse": "ClickHouse",
        "timescaledb": "TimescaleDB",
        "elasticsearch": "Elasticsearch",
        "influxdb2": "InfluxDB2",
        "influxdb3": "InfluxDB3"
    }
    name_to_container = {v: k for k, v in container_to_name.items()}

    console.print("\n" + "═" * 70)
    console.print("[bold]              STORAGE EFFICIENCY RESULTS[/]")
    console.print("═" * 70)

    console.print(f"\n[bold]Test Data:[/]")
    console.print(f"  Rows:          {rows:,}")
    console.print(f"  Raw size:      {raw_size_mb:.1f} MB (estimated)")

    # Storage table
    table = Table(title="\n📦 Storage Comparison")
    table.add_column("Database")
    table.add_column("Disk Usage", justify="right")
    table.add_column("Compression", justify="right")
    table.add_column("Bytes/Row", justify="right")
    table.add_column("Ingestion", justify="right")

    # Sort by disk usage
    sorted_dbs = sorted(storage_results.items(), key=lambda x: x[1]["mb"] if x[1]["mb"] > 0 else 999999)

    for name, storage in sorted_dbs:
        disk_mb = storage["mb"]

        if disk_mb > 0:
            compression = raw_size_mb / disk_mb
            bytes_per_row = storage["bytes"] / rows if rows > 0 else 0
        else:
            compression = 0
            bytes_per_row = 0

        ingest_time = ingest_results["results"].get(name, {}).get("time", 0)

        # Highlight best compression
        comp_str = f"{compression:.1f}x"
        valid_compressions = [raw_size_mb / s["mb"] for s in storage_results.values() if s["mb"] > 0]
        if valid_compressions and compression == max(valid_compressions):
            comp_str = f"[green]{comp_str}[/] ★"

        table.add_row(
            name,
            f"{disk_mb:.1f} MB" if disk_mb > 0 else "N/A",
            comp_str if disk_mb > 0 else "N/A",
            f"{bytes_per_row:.1f}" if disk_mb > 0 else "N/A",
            f"{ingest_time:.1f}s" if ingest_time > 0 else "N/A"
        )

    console.print(table)

    # Resource usage table
    if resources:
        res_table = Table(title="\n💻 Resource Usage During Ingestion")
        res_table.add_column("Database")
        res_table.add_column("Avg CPU %", justify="right")
        res_table.add_column("Peak CPU %", justify="right")
        res_table.add_column("Avg Memory", justify="right")
        res_table.add_column("Peak Memory", justify="right")

        # Sort by peak memory usage
        sorted_resources = sorted(resources.items(), key=lambda x: x[1].get("mem_max_mb", 0))

        for container, stats in sorted_resources:
            name = container_to_name.get(container, container)
            if stats.get("samples", 0) > 0:
                mem_avg = stats["mem_avg_mb"]
                mem_max = stats["mem_max_mb"]

                # Highlight highest memory usage
                mem_max_str = f"{mem_max:.0f} MB"
                all_mems = [s.get("mem_max_mb", 0) for s in resources.values() if s.get("samples", 0) > 0]
                if all_mems and mem_max == max(all_mems):
                    mem_max_str = f"[red]{mem_max_str}[/] ⚠️"

                res_table.add_row(
                    name,
                    f"{stats['cpu_avg']:.1f}%",
                    f"{stats['cpu_max']:.1f}%",
                    f"{mem_avg:.0f} MB",
                    mem_max_str
                )

        console.print(res_table)

    # Monthly projection
    console.print("\n[bold]📊 Monthly Cost Projection (10k DERs @ 1s)[/]")

    datapoints_per_month = 10_000 * 60 * 60 * 24 * 30  # ~26 billion

    proj_table = Table()
    proj_table.add_column("Database")
    proj_table.add_column("Storage/mo", justify="right")
    proj_table.add_column("Storage Cost", justify="right")
    proj_table.add_column("Memory Req.", justify="right")
    proj_table.add_column("Est. Compute", justify="right")
    proj_table.add_column("Total/mo", justify="right")

    for name, storage in sorted_dbs:
        if storage["mb"] > 0:
            bytes_per_row = storage["bytes"] / rows
            monthly_bytes = datapoints_per_month * bytes_per_row
            monthly_gb = monthly_bytes / (1024**3)
            storage_cost = monthly_gb * 0.10

            # Get memory requirement (use peak memory)
            container = name_to_container.get(name, name.lower())
            mem_mb = resources.get(container, {}).get("mem_max_mb", 0)

            # Estimate compute cost based on memory (rough: $0.05/GB/month for cloud)
            compute_cost = (mem_mb / 1024) * 50  # $50/GB/month for compute

            total_cost = storage_cost + compute_cost

            proj_table.add_row(
                name,
                f"{monthly_gb:.0f} GB",
                f"${storage_cost:.0f}",
                f"{mem_mb:.0f} MB",
                f"${compute_cost:.0f}",
                f"${total_cost:.0f}"
            )

    console.print(proj_table)


async def main():
    console.print("\n[bold blue]╔══════════════════════════════════════════════════════════╗[/]")
    console.print("[bold blue]║         TSDB Storage Efficiency Test                     ║[/]")
    console.print("[bold blue]║         Measuring actual disk usage                      ║[/]")
    console.print("[bold blue]╚══════════════════════════════════════════════════════════╝[/]")

    # Ingest data - 3M rows for more accurate measurement
    ingest_results = await ingest_data(rows=3_000_000)

    # Measure storage
    storage_results = measure_storage()

    # Print results
    print_results(ingest_results, storage_results)


if __name__ == "__main__":
    asyncio.run(main())
