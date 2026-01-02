"""
TSDB Benchmark Suite for Sourceful Energy
==========================================

Target: 10,000 DERs @ 1s interval = 36M datapoints/hour

Use Cases Tested:
1. INGESTION - MQTT-style high-throughput writes
2. MOBILE API - Latest values for mobile clients
3. DASHBOARD - Aggregations and downsampling
4. ANALYTICS - Complex queries, anomaly detection
5. RESOURCE USAGE - CPU, Memory, Disk for cloud cost analysis
"""

import asyncio
import json
import os
import subprocess
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Any
from decimal import Decimal

import httpx
import psycopg
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.panel import Panel

from data_generator import (
    SourcefulDataGenerator,
    DERReading,
    PVReading,
    MeterReading,
    BatteryReading,
    EVChargerReading,
)

console = Console()


# ============================================
# Data Classes
# ============================================

@dataclass
class ResourceSnapshot:
    """System resource snapshot."""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    database: str
    test_name: str
    duration_seconds: float
    rows_processed: int
    rows_per_second: float
    disk_mb: float = 0.0
    cpu_avg: float = 0.0
    memory_avg_mb: float = 0.0
    memory_peak_mb: float = 0.0
    error: str | None = None


@dataclass
class QueryResult:
    """Result from a query benchmark."""
    database: str
    query_name: str
    query_time_ms: float
    rows_returned: int
    description: str
    category: str = "general"  # mobile, dashboard, analytics
    error: str | None = None


@dataclass
class UseCaseResult:
    """Results for a specific use case."""
    name: str
    description: str
    queries: list[QueryResult] = field(default_factory=list)
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    total_time_ms: float = 0.0


@dataclass
class DatabaseBenchmark:
    """Complete benchmark results for a database."""
    name: str
    ingestion: BenchmarkResult | None = None
    disk_usage_mb: float = 0.0
    compression_ratio: float = 0.0
    use_cases: dict[str, UseCaseResult] = field(default_factory=dict)
    resource_snapshots: list[ResourceSnapshot] = field(default_factory=list)


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs."""
    num_wallets: int = 1000
    sites_per_wallet: int = 2
    devices_per_site: int = 1
    ders_per_device: int = 5

    duration_minutes: int = 5
    interval_seconds: int = 1
    batch_size: int = 10000

    # Use case iterations
    mobile_iterations: int = 100
    dashboard_iterations: int = 50
    analytics_iterations: int = 20

    @property
    def total_ders(self) -> int:
        return (self.num_wallets * self.sites_per_wallet *
                self.devices_per_site * self.ders_per_device)

    @property
    def total_datapoints(self) -> int:
        return self.total_ders * (self.duration_minutes * 60) // self.interval_seconds

    @property
    def raw_data_size_mb(self) -> float:
        # Estimate ~200 bytes per reading (JSON format)
        return (self.total_datapoints * 200) / (1024 * 1024)

    def print_config(self):
        console.print(f"\n[bold]Benchmark Configuration:[/]")
        console.print(f"  Wallets:           {self.num_wallets:,}")
        console.print(f"  Sites:             {self.num_wallets * self.sites_per_wallet:,}")
        console.print(f"  Total DERs:        {self.total_ders:,}")
        console.print(f"  Duration:          {self.duration_minutes} minutes")
        console.print(f"  Interval:          {self.interval_seconds}s")
        console.print(f"  Expected rows:     {self.total_datapoints:,}")
        console.print(f"  Raw data size:     {self.raw_data_size_mb:.1f} MB")


# ============================================
# Resource Monitor
# ============================================

class ResourceMonitor:
    """Monitor Docker container resources."""

    def __init__(self, container_name: str):
        self.container_name = container_name
        self.snapshots: list[ResourceSnapshot] = []
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self):
        """Start monitoring in background."""
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> list[ResourceSnapshot]:
        """Stop monitoring and return snapshots."""
        self._running = False
        if self._task:
            await self._task
        return self.snapshots

    async def _monitor_loop(self):
        """Monitor loop."""
        while self._running:
            try:
                snapshot = await self._get_snapshot()
                if snapshot:
                    self.snapshots.append(snapshot)
            except Exception:
                pass
            await asyncio.sleep(1)

    async def _get_snapshot(self) -> ResourceSnapshot | None:
        """Get current resource snapshot."""
        try:
            result = subprocess.run(
                ["docker", "stats", self.container_name, "--no-stream", "--format",
                 "{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode != 0:
                return None

            parts = result.stdout.strip().split(",")
            if len(parts) < 3:
                return None

            cpu = float(parts[0].replace("%", ""))
            mem_usage = parts[1].split("/")[0].strip()
            mem_mb = self._parse_memory(mem_usage)
            mem_pct = float(parts[2].replace("%", ""))

            return ResourceSnapshot(
                timestamp=time.time(),
                cpu_percent=cpu,
                memory_mb=mem_mb,
                memory_percent=mem_pct
            )
        except Exception:
            return None

    def _parse_memory(self, mem_str: str) -> float:
        """Parse memory string like '1.5GiB' or '500MiB'."""
        mem_str = mem_str.strip()
        if "GiB" in mem_str:
            return float(mem_str.replace("GiB", "")) * 1024
        elif "MiB" in mem_str:
            return float(mem_str.replace("MiB", ""))
        elif "GB" in mem_str:
            return float(mem_str.replace("GB", "")) * 1024
        elif "MB" in mem_str:
            return float(mem_str.replace("MB", ""))
        return 0.0

    def get_stats(self) -> dict:
        """Get aggregated stats."""
        if not self.snapshots:
            return {"cpu_avg": 0, "memory_avg_mb": 0, "memory_peak_mb": 0}

        cpu_values = [s.cpu_percent for s in self.snapshots]
        mem_values = [s.memory_mb for s in self.snapshots]

        return {
            "cpu_avg": sum(cpu_values) / len(cpu_values),
            "memory_avg_mb": sum(mem_values) / len(mem_values),
            "memory_peak_mb": max(mem_values)
        }


# ============================================
# Base Database Client
# ============================================

class DatabaseClient:
    """Base class for database clients."""

    name: str = "base"
    container_name: str = "base"

    async def setup(self) -> None:
        raise NotImplementedError

    async def insert_batch(self, readings: list[DERReading], generator: SourcefulDataGenerator) -> int:
        raise NotImplementedError

    async def get_disk_usage_mb(self) -> float:
        raise NotImplementedError

    async def get_row_count(self) -> int:
        raise NotImplementedError

    async def cleanup(self) -> None:
        pass

    # ==========================================
    # Use Case: MOBILE API
    # Latest values for mobile clients
    # ==========================================

    async def mobile_latest_values(self, der_ids: list[str]) -> QueryResult:
        """Get latest reading for specific DERs (mobile app home screen)."""
        raise NotImplementedError

    async def mobile_der_history(self, der_id: str, hours: int = 24) -> QueryResult:
        """Get last N hours of data for a DER (mobile app chart)."""
        raise NotImplementedError

    async def mobile_site_overview(self, site_id: str) -> QueryResult:
        """Get current state of all DERs at a site."""
        raise NotImplementedError

    # ==========================================
    # Use Case: DASHBOARD
    # Aggregations for web dashboards
    # ==========================================

    async def dashboard_site_power_now(self, site_id: str) -> QueryResult:
        """Current total power for a site (dashboard widget)."""
        raise NotImplementedError

    async def dashboard_hourly_chart(self, site_id: str, hours: int = 24) -> QueryResult:
        """Hourly aggregated data for charts."""
        raise NotImplementedError

    async def dashboard_daily_summary(self, site_id: str, days: int = 7) -> QueryResult:
        """Daily energy summary."""
        raise NotImplementedError

    async def dashboard_fleet_overview(self, wallet_id: str) -> QueryResult:
        """Overview of all sites for a wallet."""
        raise NotImplementedError

    # ==========================================
    # Use Case: ANALYTICS
    # Complex queries for analysis and AI
    # ==========================================

    async def analytics_anomaly_detection(self, threshold_pct: float = 50) -> QueryResult:
        """Find readings that deviate significantly from average."""
        raise NotImplementedError

    async def analytics_correlation(self, site_id: str) -> QueryResult:
        """Correlate PV production with consumption."""
        raise NotImplementedError

    async def analytics_peak_detection(self, site_id: str) -> QueryResult:
        """Find peak power periods."""
        raise NotImplementedError

    async def analytics_efficiency_report(self, site_id: str) -> QueryResult:
        """Calculate efficiency metrics."""
        raise NotImplementedError


# ============================================
# VictoriaMetrics Client
# ============================================

class VictoriaMetricsClient(DatabaseClient):
    """VictoriaMetrics client using InfluxDB Line Protocol."""

    name = "VictoriaMetrics"
    container_name = "victoriametrics"

    def __init__(self, url: str = "http://localhost:8428"):
        self.url = url
        self.client = httpx.AsyncClient(timeout=120.0)

    async def setup(self) -> None:
        resp = await self.client.get(f"{self.url}/health")
        resp.raise_for_status()

    async def insert_batch(self, readings: list[DERReading], generator: SourcefulDataGenerator) -> int:
        lines = [generator.to_influx_line_protocol(r) for r in readings]
        data = "\n".join(lines)
        response = await self.client.post(
            f"{self.url}/write",
            content=data,
            headers={"Content-Type": "text/plain"}
        )
        response.raise_for_status()
        return len(readings)

    async def get_disk_usage_mb(self) -> float:
        try:
            response = await self.client.get(f"{self.url}/api/v1/status/tsdb")
            data = response.json()
            size_bytes = data.get("data", {}).get("totalSizeBytes", 0)
            return size_bytes / (1024 * 1024)
        except Exception:
            return 0.0

    async def get_row_count(self) -> int:
        try:
            response = await self.client.get(f"{self.url}/api/v1/status/tsdb")
            data = response.json()
            return data.get("data", {}).get("totalSeries", 0)
        except Exception:
            return 0

    async def _promql_query(self, query: str) -> tuple[Any, float]:
        """Execute PromQL query."""
        start = time.perf_counter()
        response = await self.client.get(
            f"{self.url}/api/v1/query",
            params={"query": query}
        )
        response.raise_for_status()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return response.json(), elapsed_ms

    async def _promql_range(self, query: str, start_time: str, end_time: str, step: str) -> tuple[Any, float]:
        """Execute PromQL range query."""
        start = time.perf_counter()
        response = await self.client.get(
            f"{self.url}/api/v1/query_range",
            params={"query": query, "start": start_time, "end": end_time, "step": step}
        )
        response.raise_for_status()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return response.json(), elapsed_ms

    # Mobile API
    async def mobile_latest_values(self, der_ids: list[str]) -> QueryResult:
        # Get latest power for specific DERs
        der_filter = "|".join(der_ids[:10])  # Limit to 10 for test
        query = f'last_over_time(energy_W{{der_id=~"{der_filter}"}}[5m])'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "mobile_latest_values", time_ms, len(results),
                             "Latest power for specific DERs", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_latest_values", 0, 0, "", "mobile", str(e))

    async def mobile_der_history(self, der_id: str, hours: int = 24) -> QueryResult:
        query = f'energy_W{{der_id="{der_id}"}}[{hours}h]'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            count = sum(len(r.get("values", [])) for r in results)
            return QueryResult(self.name, "mobile_der_history", time_ms, count,
                             f"Last {hours}h history for DER", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_der_history", 0, 0, "", "mobile", str(e))

    async def mobile_site_overview(self, site_id: str) -> QueryResult:
        query = f'last_over_time(energy_W{{site_id="{site_id}"}}[5m])'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "mobile_site_overview", time_ms, len(results),
                             "Current state of all DERs at site", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_site_overview", 0, 0, "", "mobile", str(e))

    # Dashboard
    async def dashboard_site_power_now(self, site_id: str) -> QueryResult:
        query = f'sum(energy_W{{site_id="{site_id}"}})'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "dashboard_site_power_now", time_ms, len(results),
                             "Current total power for site", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_site_power_now", 0, 0, "", "dashboard", str(e))

    async def dashboard_hourly_chart(self, site_id: str, hours: int = 24) -> QueryResult:
        query = f'avg_over_time(energy_W{{site_id="{site_id}"}}[1h])'
        try:
            end = "now"
            start = f"now-{hours}h"
            data, time_ms = await self._promql_range(query, start, end, "1h")
            results = data.get("data", {}).get("result", [])
            count = sum(len(r.get("values", [])) for r in results)
            return QueryResult(self.name, "dashboard_hourly_chart", time_ms, count,
                             f"Hourly aggregated data for {hours}h", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_hourly_chart", 0, 0, "", "dashboard", str(e))

    async def dashboard_daily_summary(self, site_id: str, days: int = 7) -> QueryResult:
        query = f'sum(increase(energy_W{{site_id="{site_id}"}}[24h]))'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "dashboard_daily_summary", time_ms, len(results),
                             f"Daily summary for {days} days", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_daily_summary", 0, 0, "", "dashboard", str(e))

    async def dashboard_fleet_overview(self, wallet_id: str) -> QueryResult:
        query = f'sum by (site_id) (energy_W{{wallet_id="{wallet_id}"}})'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "dashboard_fleet_overview", time_ms, len(results),
                             "Overview of all sites for wallet", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_fleet_overview", 0, 0, "", "dashboard", str(e))

    # Analytics
    async def analytics_anomaly_detection(self, threshold_pct: float = 50) -> QueryResult:
        # Find values > 1.5x average
        query = 'energy_W{} > 1.5 * avg(energy_W{})'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "analytics_anomaly_detection", time_ms, len(results),
                             "Values > 1.5x average", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_anomaly_detection", 0, 0, "", "analytics", str(e))

    async def analytics_correlation(self, site_id: str) -> QueryResult:
        # Compare PV to consumption
        query = f'sum(energy_W{{site_id="{site_id}", der_type="pv"}}) / sum(energy_W{{site_id="{site_id}", der_type="Meter"}})'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "analytics_correlation", time_ms, len(results),
                             "PV/consumption ratio", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_correlation", 0, 0, "", "analytics", str(e))

    async def analytics_peak_detection(self, site_id: str) -> QueryResult:
        query = f'max_over_time(energy_W{{site_id="{site_id}"}}[1h])'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "analytics_peak_detection", time_ms, len(results),
                             "Peak power per hour", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_peak_detection", 0, 0, "", "analytics", str(e))

    async def analytics_efficiency_report(self, site_id: str) -> QueryResult:
        query = f'avg by (der_type) (energy_W{{site_id="{site_id}"}})'
        try:
            data, time_ms = await self._promql_query(query)
            results = data.get("data", {}).get("result", [])
            return QueryResult(self.name, "analytics_efficiency_report", time_ms, len(results),
                             "Avg power by DER type", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_efficiency_report", 0, 0, "", "analytics", str(e))

    async def cleanup(self) -> None:
        await self.client.aclose()


# ============================================
# QuestDB Client
# ============================================

class QuestDBClient(DatabaseClient):
    """QuestDB client using ILP and REST API."""

    name = "QuestDB"
    container_name = "questdb"

    def __init__(self, ilp_host: str = "localhost", ilp_port: int = 9009,
                 http_url: str = "http://localhost:9000"):
        self.ilp_host = ilp_host
        self.ilp_port = ilp_port
        self.http_url = http_url
        self.client = httpx.AsyncClient(timeout=120.0)

    async def setup(self) -> None:
        # Test connection
        response = await self.client.get(f"{self.http_url}/exec", params={"query": "SELECT 1"})
        response.raise_for_status()

        # Create table
        await self.client.get(f"{self.http_url}/exec", params={
            "query": """
                CREATE TABLE IF NOT EXISTS energy (
                    timestamp TIMESTAMP,
                    der_id SYMBOL,
                    der_type SYMBOL,
                    site_id SYMBOL,
                    device_id SYMBOL,
                    wallet_id SYMBOL,
                    make SYMBOL,
                    W LONG,
                    rated_power_W LONG,
                    mppt1_V LONG,
                    mppt1_A LONG,
                    mppt2_V LONG,
                    mppt2_A LONG,
                    total_generation_Wh LONG,
                    read_time_ms LONG,
                    L1_V DOUBLE,
                    L1_A DOUBLE,
                    L1_W LONG,
                    L2_V DOUBLE,
                    L2_A DOUBLE,
                    L2_W LONG,
                    L3_V DOUBLE,
                    L3_A DOUBLE,
                    L3_W LONG,
                    total_import_Wh LONG,
                    total_export_Wh LONG,
                    soc_percent DOUBLE,
                    voltage_V DOUBLE,
                    current_A DOUBLE,
                    temperature_C DOUBLE,
                    total_charged_Wh LONG,
                    total_discharged_Wh LONG,
                    session_Wh LONG,
                    total_Wh LONG,
                    vehicle_connected BOOLEAN,
                    vehicle_soc_percent DOUBLE
                ) timestamp(timestamp) PARTITION BY DAY WAL;
            """
        })

    async def insert_batch(self, readings: list[DERReading], generator: SourcefulDataGenerator) -> int:
        lines = [generator.to_influx_line_protocol(r) for r in readings]
        data = "\n".join(lines)

        reader, writer = await asyncio.open_connection(self.ilp_host, self.ilp_port)
        try:
            writer.write(data.encode() + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

        return len(readings)

    async def get_disk_usage_mb(self) -> float:
        try:
            query = "SELECT sum(diskSize) / 1024 / 1024 FROM table_partitions('energy')"
            response = await self.client.get(f"{self.http_url}/exec", params={"query": query})
            data = response.json()
            val = data.get("dataset", [[0]])[0][0]
            return float(val) if val else 0.0
        except Exception:
            return 0.0

    async def get_row_count(self) -> int:
        try:
            query = "SELECT count() FROM energy"
            response = await self.client.get(f"{self.http_url}/exec", params={"query": query})
            data = response.json()
            return int(data.get("dataset", [[0]])[0][0] or 0)
        except Exception:
            return 0

    async def _sql_query(self, query: str) -> tuple[Any, float, int]:
        """Execute SQL query."""
        start = time.perf_counter()
        response = await self.client.get(f"{self.http_url}/exec", params={"query": query})
        response.raise_for_status()
        elapsed_ms = (time.perf_counter() - start) * 1000
        data = response.json()
        rows = data.get("dataset", [])
        return data, elapsed_ms, len(rows)

    # Mobile API
    async def mobile_latest_values(self, der_ids: list[str]) -> QueryResult:
        der_list = "', '".join(der_ids[:10])
        query = f"""
            SELECT * FROM energy
            WHERE der_id IN ('{der_list}')
            LATEST ON timestamp PARTITION BY der_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_latest_values", time_ms, count,
                             "Latest reading for specific DERs", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_latest_values", 0, 0, "", "mobile", str(e))

    async def mobile_der_history(self, der_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT timestamp, W, der_type FROM energy
            WHERE der_id = '{der_id}'
            AND timestamp > dateadd('h', -{hours}, now())
            ORDER BY timestamp
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_der_history", time_ms, count,
                             f"Last {hours}h history for DER", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_der_history", 0, 0, "", "mobile", str(e))

    async def mobile_site_overview(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT * FROM energy
            WHERE site_id = '{site_id}'
            LATEST ON timestamp PARTITION BY der_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_site_overview", time_ms, count,
                             "Current state of all DERs at site", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_site_overview", 0, 0, "", "mobile", str(e))

    # Dashboard
    async def dashboard_site_power_now(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT sum(W) as total_power FROM energy
            WHERE site_id = '{site_id}'
            LATEST ON timestamp PARTITION BY der_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_site_power_now", time_ms, count,
                             "Current total power for site", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_site_power_now", 0, 0, "", "dashboard", str(e))

    async def dashboard_hourly_chart(self, site_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT timestamp, avg(W) as avg_power, sum(W) as total_power
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > dateadd('h', -{hours}, now())
            SAMPLE BY 1h ALIGN TO CALENDAR
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_hourly_chart", time_ms, count,
                             f"Hourly aggregated data for {hours}h", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_hourly_chart", 0, 0, "", "dashboard", str(e))

    async def dashboard_daily_summary(self, site_id: str, days: int = 7) -> QueryResult:
        query = f"""
            SELECT timestamp,
                   sum(W) / 1000 as total_kW,
                   count() as readings
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > dateadd('d', -{days}, now())
            SAMPLE BY 1d ALIGN TO CALENDAR
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_daily_summary", time_ms, count,
                             f"Daily summary for {days} days", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_daily_summary", 0, 0, "", "dashboard", str(e))

    async def dashboard_fleet_overview(self, wallet_id: str) -> QueryResult:
        query = f"""
            SELECT site_id, sum(W) as total_power, count() as der_count
            FROM energy
            WHERE wallet_id = '{wallet_id}'
            LATEST ON timestamp PARTITION BY der_id
            GROUP BY site_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_fleet_overview", time_ms, count,
                             "Overview of all sites for wallet", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_fleet_overview", 0, 0, "", "dashboard", str(e))

    # Analytics
    async def analytics_anomaly_detection(self, threshold_pct: float = 50) -> QueryResult:
        query = f"""
            WITH stats AS (
                SELECT avg(W) as avg_w, stddev(W) as std_w FROM energy WHERE W > 0
            )
            SELECT e.timestamp, e.der_id, e.W, e.der_type
            FROM energy e, stats s
            WHERE e.W > s.avg_w + (s.std_w * 2)
            LIMIT 100
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_anomaly_detection", time_ms, count,
                             "Values > 2 stddev from mean", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_anomaly_detection", 0, 0, "", "analytics", str(e))

    async def analytics_correlation(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT timestamp,
                   sum(CASE WHEN der_type = 'pv' THEN W ELSE 0 END) as pv_power,
                   sum(CASE WHEN der_type = 'Meter' THEN W ELSE 0 END) as consumption
            FROM energy
            WHERE site_id = '{site_id}'
            SAMPLE BY 5m ALIGN TO CALENDAR
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_correlation", time_ms, count,
                             "PV vs consumption by 5min", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_correlation", 0, 0, "", "analytics", str(e))

    async def analytics_peak_detection(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT timestamp, max(W) as peak_power, der_type
            FROM energy
            WHERE site_id = '{site_id}'
            SAMPLE BY 1h ALIGN TO CALENDAR
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_peak_detection", time_ms, count,
                             "Peak power per hour", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_peak_detection", 0, 0, "", "analytics", str(e))

    async def analytics_efficiency_report(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT der_type,
                   avg(W) as avg_power,
                   min(W) as min_power,
                   max(W) as max_power,
                   count() as readings
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY der_type
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_efficiency_report", time_ms, count,
                             "Power stats by DER type", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_efficiency_report", 0, 0, "", "analytics", str(e))

    async def cleanup(self) -> None:
        await self.client.aclose()


# ============================================
# ClickHouse Client
# ============================================

class ClickHouseClient(DatabaseClient):
    """ClickHouse client using HTTP interface."""

    name = "ClickHouse"
    container_name = "clickhouse"

    def __init__(self, url: str = "http://localhost:8123"):
        self.base_url = url
        self.client = httpx.AsyncClient(timeout=120.0)

    async def setup(self) -> None:
        # Test connection
        response = await self.client.post(self.base_url, content="SELECT 1")
        response.raise_for_status()

        # Create table with MergeTree for time-series
        create_table = """
            CREATE TABLE IF NOT EXISTS energy (
                timestamp DateTime64(3),
                der_id String,
                der_type LowCardinality(String),
                site_id LowCardinality(String),
                device_id LowCardinality(String),
                wallet_id LowCardinality(String),
                make LowCardinality(String),
                W Int64,
                rated_power_W Int64 DEFAULT 0,
                mppt1_V Int64 DEFAULT 0,
                mppt1_A Int64 DEFAULT 0,
                mppt2_V Int64 DEFAULT 0,
                mppt2_A Int64 DEFAULT 0,
                total_generation_Wh Int64 DEFAULT 0,
                read_time_ms Int64 DEFAULT 0,
                L1_V Float64 DEFAULT 0,
                L1_A Float64 DEFAULT 0,
                L1_W Int64 DEFAULT 0,
                L2_V Float64 DEFAULT 0,
                L2_A Float64 DEFAULT 0,
                L2_W Int64 DEFAULT 0,
                L3_V Float64 DEFAULT 0,
                L3_A Float64 DEFAULT 0,
                L3_W Int64 DEFAULT 0,
                total_import_Wh Int64 DEFAULT 0,
                total_export_Wh Int64 DEFAULT 0,
                soc_percent Float64 DEFAULT 0,
                voltage_V Float64 DEFAULT 0,
                current_A Float64 DEFAULT 0,
                temperature_C Float64 DEFAULT 0,
                total_charged_Wh Int64 DEFAULT 0,
                total_discharged_Wh Int64 DEFAULT 0,
                session_Wh Int64 DEFAULT 0,
                total_Wh Int64 DEFAULT 0,
                vehicle_connected UInt8 DEFAULT 0,
                vehicle_soc_percent Float64 DEFAULT 0
            ) ENGINE = MergeTree()
            ORDER BY (site_id, der_id, timestamp)
            PARTITION BY toYYYYMMDD(timestamp)
        """
        await self.client.post(self.base_url, content=create_table)

    def _reading_to_json_row(self, reading: DERReading) -> dict:
        """Convert reading to JSON row."""
        ts_str = reading.timestamp.rstrip("Z")
        dt = datetime.fromisoformat(ts_str)
        ts_formatted = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        row = {
            "timestamp": ts_formatted,
            "der_id": reading.der_id,
            "der_type": reading.type,
            "site_id": reading.site_id,
            "device_id": reading.device_id,
            "wallet_id": reading.wallet_id,
            "make": reading.make,
            "W": reading.W,
        }

        if isinstance(reading, PVReading):
            row.update({
                "rated_power_W": reading.rated_power_W,
                "mppt1_V": reading.mppt1_V,
                "mppt1_A": reading.mppt1_A,
                "mppt2_V": reading.mppt2_V,
                "mppt2_A": reading.mppt2_A,
                "total_generation_Wh": reading.total_generation_Wh,
                "read_time_ms": reading.read_time_ms,
            })
        elif isinstance(reading, MeterReading):
            row.update({
                "L1_V": reading.L1_V,
                "L1_A": reading.L1_A,
                "L1_W": reading.L1_W,
                "L2_V": reading.L2_V,
                "L2_A": reading.L2_A,
                "L2_W": reading.L2_W,
                "L3_V": reading.L3_V,
                "L3_A": reading.L3_A,
                "L3_W": reading.L3_W,
                "total_import_Wh": reading.total_import_Wh,
                "total_export_Wh": reading.total_export_Wh,
            })
        elif isinstance(reading, BatteryReading):
            row.update({
                "soc_percent": reading.soc_percent,
                "voltage_V": reading.voltage_V,
                "current_A": reading.current_A,
                "temperature_C": reading.temperature_C,
                "total_charged_Wh": reading.total_charged_Wh,
                "total_discharged_Wh": reading.total_discharged_Wh,
            })
        elif isinstance(reading, EVChargerReading):
            row.update({
                "voltage_V": reading.voltage_V,
                "current_A": reading.current_A,
                "session_Wh": reading.session_Wh,
                "total_Wh": reading.total_Wh,
                "vehicle_connected": 1 if reading.vehicle_connected else 0,
                "vehicle_soc_percent": reading.vehicle_soc_percent or 0,
            })

        return row

    async def insert_batch(self, readings: list[DERReading], generator: SourcefulDataGenerator) -> int:
        rows = [self._reading_to_json_row(r) for r in readings]
        json_lines = "\n".join(json.dumps(row) for row in rows)

        response = await self.client.post(
            f"{self.base_url}/?query=INSERT INTO energy FORMAT JSONEachRow",
            content=json_lines,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return len(readings)

    async def get_disk_usage_mb(self) -> float:
        try:
            query = "SELECT sum(bytes_on_disk) / 1024 / 1024 as mb FROM system.parts WHERE table = 'energy' AND active FORMAT JSON"
            response = await self.client.post(self.base_url, content=query)
            data = response.json()
            rows = data.get("data", [])
            return float(rows[0]["mb"]) if rows else 0.0
        except Exception:
            return 0.0

    async def get_row_count(self) -> int:
        try:
            query = "SELECT count() as cnt FROM energy FORMAT JSON"
            response = await self.client.post(self.base_url, content=query)
            data = response.json()
            rows = data.get("data", [])
            return int(rows[0]["cnt"]) if rows else 0
        except Exception:
            return 0

    async def _sql_query(self, query: str) -> tuple[Any, float, int]:
        """Execute SQL query."""
        start = time.perf_counter()
        response = await self.client.post(self.base_url, content=query + " FORMAT JSON")
        response.raise_for_status()
        elapsed_ms = (time.perf_counter() - start) * 1000
        data = response.json()
        rows = data.get("data", [])
        return data, elapsed_ms, len(rows)

    # Mobile API
    async def mobile_latest_values(self, der_ids: list[str]) -> QueryResult:
        der_list = "', '".join(der_ids[:10])
        query = f"""
            SELECT der_id, argMax(W, timestamp) as W, max(timestamp) as ts
            FROM energy
            WHERE der_id IN ('{der_list}')
            GROUP BY der_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_latest_values", time_ms, count,
                             "Latest reading for specific DERs", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_latest_values", 0, 0, "", "mobile", str(e))

    async def mobile_der_history(self, der_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT timestamp, W, der_type
            FROM energy
            WHERE der_id = '{der_id}'
            AND timestamp > now() - INTERVAL {hours} HOUR
            ORDER BY timestamp
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_der_history", time_ms, count,
                             f"Last {hours}h history for DER", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_der_history", 0, 0, "", "mobile", str(e))

    async def mobile_site_overview(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT der_id, der_type, argMax(W, timestamp) as W, max(timestamp) as ts
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY der_id, der_type
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_site_overview", time_ms, count,
                             "Current state of all DERs at site", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_site_overview", 0, 0, "", "mobile", str(e))

    # Dashboard
    async def dashboard_site_power_now(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT sum(latest_w) as total_power FROM (
                SELECT der_id, argMax(W, timestamp) as latest_w
                FROM energy
                WHERE site_id = '{site_id}'
                GROUP BY der_id
            )
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_site_power_now", time_ms, count,
                             "Current total power for site", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_site_power_now", 0, 0, "", "dashboard", str(e))

    async def dashboard_hourly_chart(self, site_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT
                toStartOfHour(timestamp) as hour,
                avg(W) as avg_power,
                sum(W) as total_power,
                count() as readings
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > now() - INTERVAL {hours} HOUR
            GROUP BY hour
            ORDER BY hour
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_hourly_chart", time_ms, count,
                             f"Hourly aggregated data for {hours}h", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_hourly_chart", 0, 0, "", "dashboard", str(e))

    async def dashboard_daily_summary(self, site_id: str, days: int = 7) -> QueryResult:
        query = f"""
            SELECT
                toDate(timestamp) as day,
                sum(W) / 1000 as total_kW,
                count() as readings
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > now() - INTERVAL {days} DAY
            GROUP BY day
            ORDER BY day
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_daily_summary", time_ms, count,
                             f"Daily summary for {days} days", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_daily_summary", 0, 0, "", "dashboard", str(e))

    async def dashboard_fleet_overview(self, wallet_id: str) -> QueryResult:
        query = f"""
            SELECT site_id, sum(latest_w) as total_power, count() as der_count
            FROM (
                SELECT site_id, der_id, argMax(W, timestamp) as latest_w
                FROM energy
                WHERE wallet_id = '{wallet_id}'
                GROUP BY site_id, der_id
            )
            GROUP BY site_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_fleet_overview", time_ms, count,
                             "Overview of all sites for wallet", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_fleet_overview", 0, 0, "", "dashboard", str(e))

    # Analytics
    async def analytics_anomaly_detection(self, threshold_pct: float = 50) -> QueryResult:
        query = """
            WITH stats AS (
                SELECT avg(W) as avg_w, stddevPop(W) as std_w FROM energy WHERE W > 0
            )
            SELECT timestamp, der_id, W, der_type
            FROM energy, stats
            WHERE W > avg_w + (std_w * 2)
            LIMIT 100
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_anomaly_detection", time_ms, count,
                             "Values > 2 stddev from mean", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_anomaly_detection", 0, 0, "", "analytics", str(e))

    async def analytics_correlation(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                toStartOfFiveMinutes(timestamp) as ts,
                sumIf(W, der_type = 'pv') as pv_power,
                sumIf(W, der_type = 'Meter') as consumption
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY ts
            ORDER BY ts
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_correlation", time_ms, count,
                             "PV vs consumption by 5min", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_correlation", 0, 0, "", "analytics", str(e))

    async def analytics_peak_detection(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                toStartOfHour(timestamp) as hour,
                max(W) as peak_power,
                argMax(der_type, W) as peak_der_type
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY hour
            ORDER BY hour
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_peak_detection", time_ms, count,
                             "Peak power per hour", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_peak_detection", 0, 0, "", "analytics", str(e))

    async def analytics_efficiency_report(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                der_type,
                avg(W) as avg_power,
                min(W) as min_power,
                max(W) as max_power,
                stddevPop(W) as stddev_power,
                count() as readings
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY der_type
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_efficiency_report", time_ms, count,
                             "Power stats by DER type", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_efficiency_report", 0, 0, "", "analytics", str(e))

    async def cleanup(self) -> None:
        await self.client.aclose()


# ============================================
# TimescaleDB Client
# ============================================

class TimescaleDBClient(DatabaseClient):
    """TimescaleDB client."""

    name = "TimescaleDB"
    container_name = "timescaledb"

    def __init__(self, host: str = "localhost", port: int = 5432):
        self.host = host
        self.port = port
        self.conn = None

    async def setup(self) -> None:
        self.conn = await psycopg.AsyncConnection.connect(
            f"postgresql://postgres:postgres@{self.host}:{self.port}/sourceful"
        )

        async with self.conn.cursor() as cur:
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
                    W BIGINT,
                    rated_power_W BIGINT DEFAULT 0,
                    mppt1_V BIGINT DEFAULT 0,
                    mppt1_A BIGINT DEFAULT 0,
                    mppt2_V BIGINT DEFAULT 0,
                    mppt2_A BIGINT DEFAULT 0,
                    total_generation_Wh BIGINT DEFAULT 0,
                    read_time_ms BIGINT DEFAULT 0,
                    L1_V DOUBLE PRECISION DEFAULT 0,
                    L1_A DOUBLE PRECISION DEFAULT 0,
                    L1_W BIGINT DEFAULT 0,
                    L2_V DOUBLE PRECISION DEFAULT 0,
                    L2_A DOUBLE PRECISION DEFAULT 0,
                    L2_W BIGINT DEFAULT 0,
                    L3_V DOUBLE PRECISION DEFAULT 0,
                    L3_A DOUBLE PRECISION DEFAULT 0,
                    L3_W BIGINT DEFAULT 0,
                    total_import_Wh BIGINT DEFAULT 0,
                    total_export_Wh BIGINT DEFAULT 0,
                    soc_percent DOUBLE PRECISION DEFAULT 0,
                    voltage_V DOUBLE PRECISION DEFAULT 0,
                    current_A DOUBLE PRECISION DEFAULT 0,
                    temperature_C DOUBLE PRECISION DEFAULT 0,
                    total_charged_Wh BIGINT DEFAULT 0,
                    total_discharged_Wh BIGINT DEFAULT 0,
                    session_Wh BIGINT DEFAULT 0,
                    total_Wh BIGINT DEFAULT 0,
                    vehicle_connected BOOLEAN DEFAULT FALSE,
                    vehicle_soc_percent DOUBLE PRECISION DEFAULT 0
                );
            """)

            # Create hypertable
            try:
                await cur.execute("""
                    SELECT create_hypertable('energy', 'timestamp',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 day'
                    );
                """)
            except Exception:
                pass

            # Create indexes for common queries
            await cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_energy_der_id ON energy (der_id, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_energy_site_id ON energy (site_id, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_energy_wallet_id ON energy (wallet_id, timestamp DESC);
            """)

        await self.conn.commit()

    def _reading_to_tuple(self, reading: DERReading) -> tuple:
        """Convert reading to tuple for insertion."""
        ts_str = reading.timestamp.rstrip("Z")
        dt = datetime.fromisoformat(ts_str)

        base = (
            dt, reading.der_id, reading.type, reading.site_id,
            reading.device_id, reading.wallet_id, reading.make, reading.W
        )

        # Need 28 values after base (8 base + 28 = 36 total columns)
        # Columns after base: rated_power_W, mppt1_V, mppt1_A, mppt2_V, mppt2_A, total_generation_Wh, read_time_ms,
        #   L1_V, L1_A, L1_W, L2_V, L2_A, L2_W, L3_V, L3_A, L3_W, total_import_Wh, total_export_Wh,
        #   soc_percent, voltage_V, current_A, temperature_C, total_charged_Wh, total_discharged_Wh,
        #   session_Wh, total_Wh, vehicle_connected, vehicle_soc_percent
        if isinstance(reading, PVReading):
            return base + (
                reading.rated_power_W, reading.mppt1_V, reading.mppt1_A,
                reading.mppt2_V, reading.mppt2_A, reading.total_generation_Wh,
                reading.read_time_ms,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  # L1-L3, import/export (11)
                0, 0, 0, 0, 0, 0,  # soc, voltage, current, temp, charged, discharged (6)
                0, 0, False, 0  # session, total, connected, soc (4)
            )
        elif isinstance(reading, MeterReading):
            return base + (
                0, 0, 0, 0, 0, 0, 0,  # PV fields (7)
                reading.L1_V, reading.L1_A, reading.L1_W,
                reading.L2_V, reading.L2_A, reading.L2_W,
                reading.L3_V, reading.L3_A, reading.L3_W,
                reading.total_import_Wh, reading.total_export_Wh,
                0, 0, 0, 0, 0, 0,  # battery fields (6)
                0, 0, False, 0  # EV fields (4)
            )
        elif isinstance(reading, BatteryReading):
            return base + (
                0, 0, 0, 0, 0, 0, 0,  # PV fields (7)
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  # Meter fields (11)
                reading.soc_percent, reading.voltage_V, reading.current_A,
                reading.temperature_C, reading.total_charged_Wh,
                reading.total_discharged_Wh,
                0, 0, False, 0  # EV fields (4)
            )
        else:  # EVChargerReading
            return base + (
                0, 0, 0, 0, 0, 0, 0,  # PV fields (7)
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  # Meter fields (11)
                0, reading.voltage_V, reading.current_A, 0, 0, 0,  # Battery-like fields (6)
                reading.session_Wh, reading.total_Wh,
                reading.vehicle_connected, reading.vehicle_soc_percent or 0
            )

    async def insert_batch(self, readings: list[DERReading], generator: SourcefulDataGenerator) -> int:
        tuples = [self._reading_to_tuple(r) for r in readings]

        async with self.conn.cursor() as cur:
            await cur.executemany("""
                INSERT INTO energy (
                    timestamp, der_id, der_type, site_id, device_id, wallet_id, make, W,
                    rated_power_W, mppt1_V, mppt1_A, mppt2_V, mppt2_A, total_generation_Wh, read_time_ms,
                    L1_V, L1_A, L1_W, L2_V, L2_A, L2_W, L3_V, L3_A, L3_W,
                    total_import_Wh, total_export_Wh,
                    soc_percent, voltage_V, current_A, temperature_C,
                    total_charged_Wh, total_discharged_Wh,
                    session_Wh, total_Wh, vehicle_connected, vehicle_soc_percent
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, tuples)

        await self.conn.commit()
        return len(readings)

    async def get_disk_usage_mb(self) -> float:
        try:
            async with self.conn.cursor() as cur:
                await cur.execute("""
                    SELECT pg_total_relation_size('energy') / 1024.0 / 1024.0
                """)
                result = await cur.fetchone()
            return float(result[0]) if result else 0.0
        except Exception:
            return 0.0

    async def get_row_count(self) -> int:
        try:
            async with self.conn.cursor() as cur:
                await cur.execute("SELECT count(*) FROM energy")
                result = await cur.fetchone()
            return int(result[0]) if result else 0
        except Exception:
            return 0

    async def _sql_query(self, query: str) -> tuple[list, float, int]:
        start = time.perf_counter()
        async with self.conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return rows, elapsed_ms, len(rows)

    # Mobile API
    async def mobile_latest_values(self, der_ids: list[str]) -> QueryResult:
        der_list = "', '".join(der_ids[:10])
        query = f"""
            SELECT DISTINCT ON (der_id) der_id, W, timestamp, der_type
            FROM energy
            WHERE der_id IN ('{der_list}')
            ORDER BY der_id, timestamp DESC
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_latest_values", time_ms, count,
                             "Latest reading for specific DERs", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_latest_values", 0, 0, "", "mobile", str(e))

    async def mobile_der_history(self, der_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT timestamp, W, der_type
            FROM energy
            WHERE der_id = '{der_id}'
            AND timestamp > NOW() - INTERVAL '{hours} hours'
            ORDER BY timestamp
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_der_history", time_ms, count,
                             f"Last {hours}h history for DER", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_der_history", 0, 0, "", "mobile", str(e))

    async def mobile_site_overview(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT DISTINCT ON (der_id) der_id, der_type, W, timestamp
            FROM energy
            WHERE site_id = '{site_id}'
            ORDER BY der_id, timestamp DESC
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "mobile_site_overview", time_ms, count,
                             "Current state of all DERs at site", "mobile")
        except Exception as e:
            return QueryResult(self.name, "mobile_site_overview", 0, 0, "", "mobile", str(e))

    # Dashboard
    async def dashboard_site_power_now(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT sum(latest_w) as total_power FROM (
                SELECT DISTINCT ON (der_id) W as latest_w
                FROM energy
                WHERE site_id = '{site_id}'
                ORDER BY der_id, timestamp DESC
            ) sub
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_site_power_now", time_ms, count,
                             "Current total power for site", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_site_power_now", 0, 0, "", "dashboard", str(e))

    async def dashboard_hourly_chart(self, site_id: str, hours: int = 24) -> QueryResult:
        query = f"""
            SELECT
                time_bucket('1 hour', timestamp) as hour,
                avg(W) as avg_power,
                sum(W) as total_power,
                count(*) as readings
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > NOW() - INTERVAL '{hours} hours'
            GROUP BY hour
            ORDER BY hour
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_hourly_chart", time_ms, count,
                             f"Hourly aggregated data for {hours}h", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_hourly_chart", 0, 0, "", "dashboard", str(e))

    async def dashboard_daily_summary(self, site_id: str, days: int = 7) -> QueryResult:
        query = f"""
            SELECT
                time_bucket('1 day', timestamp) as day,
                sum(W) / 1000 as total_kW,
                count(*) as readings
            FROM energy
            WHERE site_id = '{site_id}'
            AND timestamp > NOW() - INTERVAL '{days} days'
            GROUP BY day
            ORDER BY day
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_daily_summary", time_ms, count,
                             f"Daily summary for {days} days", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_daily_summary", 0, 0, "", "dashboard", str(e))

    async def dashboard_fleet_overview(self, wallet_id: str) -> QueryResult:
        query = f"""
            SELECT site_id, sum(latest_w) as total_power, count(*) as der_count
            FROM (
                SELECT DISTINCT ON (der_id) site_id, W as latest_w
                FROM energy
                WHERE wallet_id = '{wallet_id}'
                ORDER BY der_id, timestamp DESC
            ) sub
            GROUP BY site_id
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "dashboard_fleet_overview", time_ms, count,
                             "Overview of all sites for wallet", "dashboard")
        except Exception as e:
            return QueryResult(self.name, "dashboard_fleet_overview", 0, 0, "", "dashboard", str(e))

    # Analytics
    async def analytics_anomaly_detection(self, threshold_pct: float = 50) -> QueryResult:
        query = """
            WITH stats AS (
                SELECT avg(W) as avg_w, stddev(W) as std_w FROM energy WHERE W > 0
            )
            SELECT e.timestamp, e.der_id, e.W, e.der_type
            FROM energy e, stats s
            WHERE e.W > s.avg_w + (s.std_w * 2)
            LIMIT 100
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_anomaly_detection", time_ms, count,
                             "Values > 2 stddev from mean", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_anomaly_detection", 0, 0, "", "analytics", str(e))

    async def analytics_correlation(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                time_bucket('5 minutes', timestamp) as ts,
                sum(CASE WHEN der_type = 'pv' THEN W ELSE 0 END) as pv_power,
                sum(CASE WHEN der_type = 'Meter' THEN W ELSE 0 END) as consumption
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY ts
            ORDER BY ts
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_correlation", time_ms, count,
                             "PV vs consumption by 5min", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_correlation", 0, 0, "", "analytics", str(e))

    async def analytics_peak_detection(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                time_bucket('1 hour', timestamp) as hour,
                max(W) as peak_power
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY hour
            ORDER BY hour
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_peak_detection", time_ms, count,
                             "Peak power per hour", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_peak_detection", 0, 0, "", "analytics", str(e))

    async def analytics_efficiency_report(self, site_id: str) -> QueryResult:
        query = f"""
            SELECT
                der_type,
                avg(W) as avg_power,
                min(W) as min_power,
                max(W) as max_power,
                stddev(W) as stddev_power,
                count(*) as readings
            FROM energy
            WHERE site_id = '{site_id}'
            GROUP BY der_type
        """
        try:
            _, time_ms, count = await self._sql_query(query)
            return QueryResult(self.name, "analytics_efficiency_report", time_ms, count,
                             "Power stats by DER type", "analytics")
        except Exception as e:
            return QueryResult(self.name, "analytics_efficiency_report", 0, 0, "", "analytics", str(e))

    async def cleanup(self) -> None:
        if self.conn:
            await self.conn.close()


# ============================================
# Benchmark Runner
# ============================================

class BenchmarkRunner:
    """Main benchmark runner."""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.results: dict[str, DatabaseBenchmark] = {}
        self.generator = SourcefulDataGenerator(
            num_wallets=config.num_wallets,
            sites_per_wallet=config.sites_per_wallet,
            devices_per_site=config.devices_per_site,
            ders_per_device=config.ders_per_device
        )

    async def run_ingestion(self, client: DatabaseClient) -> BenchmarkResult:
        """Run ingestion benchmark with resource monitoring."""
        console.print(f"\n[bold cyan]▶ Ingestion: {client.name}[/]")

        # Start resource monitor
        monitor = ResourceMonitor(client.container_name)
        await monitor.start()

        try:
            await client.setup()
            console.print("  ✓ Schema ready")
        except Exception as e:
            await monitor.stop()
            return BenchmarkResult(client.name, "ingestion", 0, 0, 0, error=str(e))

        total_seconds = self.config.duration_minutes * 60
        readings_generated = 0
        start_time_perf = time.perf_counter()
        base_time = datetime.now() - timedelta(minutes=self.config.duration_minutes)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed:,}/{task.total:,})"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(client.name, total=self.config.total_datapoints)

            # Generate all readings using the generator
            all_readings = list(self.generator.generate_batch(
                start_time=base_time,
                duration_seconds=total_seconds,
                interval_seconds=self.config.interval_seconds
            ))

            # Send in batches
            for i in range(0, len(all_readings), self.config.batch_size):
                chunk = all_readings[i:i + self.config.batch_size]
                await client.insert_batch(chunk, self.generator)
                readings_generated += len(chunk)
                progress.update(task, completed=readings_generated)

        elapsed = time.perf_counter() - start_time_perf

        # Stop monitoring and get stats
        await monitor.stop()
        stats = monitor.get_stats()

        # Get disk usage
        await asyncio.sleep(2)  # Wait for writes to flush
        disk_mb = await client.get_disk_usage_mb()

        result = BenchmarkResult(
            database=client.name,
            test_name="ingestion",
            duration_seconds=elapsed,
            rows_processed=readings_generated,
            rows_per_second=readings_generated / elapsed,
            disk_mb=disk_mb,
            cpu_avg=stats["cpu_avg"],
            memory_avg_mb=stats["memory_avg_mb"],
            memory_peak_mb=stats["memory_peak_mb"]
        )

        console.print(f"  ✓ {readings_generated:,} rows in {elapsed:.1f}s = [green]{result.rows_per_second:,.0f} rows/sec[/]")
        if disk_mb > 0:
            compression = (self.config.raw_data_size_mb / disk_mb) if disk_mb > 0 else 0
            console.print(f"  ✓ Disk: {disk_mb:.1f} MB (compression: {compression:.1f}x)")
        console.print(f"  ✓ Resources: CPU avg {stats['cpu_avg']:.1f}%, Memory avg {stats['memory_avg_mb']:.0f} MB, peak {stats['memory_peak_mb']:.0f} MB")

        return result

    async def run_use_case(self, client: DatabaseClient, use_case: str) -> UseCaseResult:
        """Run use case benchmark."""

        # Get sample IDs for testing (DERConfig objects have attributes, not dict keys)
        der_ids = [d.der_id for d in self.generator.ders[:100]]
        site_id = self.generator.ders[0].site_id
        wallet_id = self.generator.ders[0].wallet_id

        queries = []

        if use_case == "mobile":
            # Mobile API queries - simulate many concurrent users
            for _ in range(self.config.mobile_iterations):
                queries.append(await client.mobile_latest_values(der_ids[:10]))
                queries.append(await client.mobile_der_history(der_ids[0]))
                queries.append(await client.mobile_site_overview(site_id))

        elif use_case == "dashboard":
            # Dashboard queries - simulate web dashboard refreshes
            for _ in range(self.config.dashboard_iterations):
                queries.append(await client.dashboard_site_power_now(site_id))
                queries.append(await client.dashboard_hourly_chart(site_id))
                queries.append(await client.dashboard_daily_summary(site_id))
                queries.append(await client.dashboard_fleet_overview(wallet_id))

        elif use_case == "analytics":
            # Analytics queries - complex analysis
            for _ in range(self.config.analytics_iterations):
                queries.append(await client.analytics_anomaly_detection())
                queries.append(await client.analytics_correlation(site_id))
                queries.append(await client.analytics_peak_detection(site_id))
                queries.append(await client.analytics_efficiency_report(site_id))

        # Calculate stats
        times = [q.query_time_ms for q in queries if q.error is None]
        if times:
            times_sorted = sorted(times)
            avg_latency = sum(times) / len(times)
            p95_idx = int(len(times_sorted) * 0.95)
            p95_latency = times_sorted[p95_idx] if p95_idx < len(times_sorted) else times_sorted[-1]
            total_time = sum(times)
        else:
            avg_latency = p95_latency = total_time = 0

        return UseCaseResult(
            name=use_case,
            description=f"{use_case.title()} use case queries",
            queries=queries,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            total_time_ms=total_time
        )

    async def run_database(self, client: DatabaseClient) -> DatabaseBenchmark:
        """Run all benchmarks for a database."""
        benchmark = DatabaseBenchmark(name=client.name)

        # Ingestion
        benchmark.ingestion = await self.run_ingestion(client)
        if benchmark.ingestion.error:
            console.print(f"  [red]✗ Ingestion failed: {benchmark.ingestion.error}[/]")
            return benchmark

        benchmark.disk_usage_mb = benchmark.ingestion.disk_mb
        if benchmark.disk_usage_mb > 0:
            benchmark.compression_ratio = self.config.raw_data_size_mb / benchmark.disk_usage_mb

        # Use cases
        for use_case in ["mobile", "dashboard", "analytics"]:
            console.print(f"\n[bold cyan]▶ {use_case.title()} Use Case: {client.name}[/]")
            result = await self.run_use_case(client, use_case)
            benchmark.use_cases[use_case] = result

            errors = sum(1 for q in result.queries if q.error)
            console.print(f"  ✓ {len(result.queries)} queries, avg: {result.avg_latency_ms:.1f}ms, p95: {result.p95_latency_ms:.1f}ms")
            if errors:
                console.print(f"  ⚠ {errors} errors")

        await client.cleanup()
        return benchmark

    async def run_all(self) -> dict[str, DatabaseBenchmark]:
        """Run benchmarks for all databases."""
        clients = [
            VictoriaMetricsClient(),
            QuestDBClient(),
            ClickHouseClient(),
            TimescaleDBClient(),
        ]

        for client in clients:
            try:
                self.results[client.name] = await self.run_database(client)
            except Exception as e:
                console.print(f"[red]Error with {client.name}: {e}[/]")

        return self.results

    def print_results(self):
        """Print formatted results."""
        console.print("\n" + "═" * 70)
        console.print("[bold]                    BENCHMARK RESULTS[/]")
        console.print("═" * 70)

        # Ingestion table
        console.print("\n[bold]📥 Ingestion Performance[/]")
        table = Table()
        table.add_column("Database")
        table.add_column("Rows/sec", justify="right")
        table.add_column("Time", justify="right")
        table.add_column("Disk MB", justify="right")
        table.add_column("Compression", justify="right")
        table.add_column("CPU Avg", justify="right")
        table.add_column("Mem Avg", justify="right")
        table.add_column("Mem Peak", justify="right")

        for name, bench in sorted(self.results.items(),
                                   key=lambda x: x[1].ingestion.rows_per_second if x[1].ingestion else 0,
                                   reverse=True):
            if bench.ingestion and not bench.ingestion.error:
                table.add_row(
                    name,
                    f"{bench.ingestion.rows_per_second:,.0f}",
                    f"{bench.ingestion.duration_seconds:.1f}s",
                    f"{bench.disk_usage_mb:.1f}" if bench.disk_usage_mb else "-",
                    f"{bench.compression_ratio:.1f}x" if bench.compression_ratio else "-",
                    f"{bench.ingestion.cpu_avg:.1f}%",
                    f"{bench.ingestion.memory_avg_mb:.0f}",
                    f"{bench.ingestion.memory_peak_mb:.0f}",
                )
        console.print(table)

        # Use case tables
        for use_case in ["mobile", "dashboard", "analytics"]:
            console.print(f"\n[bold]📱 {use_case.title()} Use Case (latency in ms)[/]")
            table = Table()
            table.add_column("Database")
            table.add_column("Avg", justify="right")
            table.add_column("P95", justify="right")
            table.add_column("Total", justify="right")
            table.add_column("Queries", justify="right")
            table.add_column("Errors", justify="right")

            items = [(name, bench.use_cases.get(use_case))
                     for name, bench in self.results.items()
                     if use_case in bench.use_cases]
            items.sort(key=lambda x: x[1].avg_latency_ms if x[1] else float('inf'))

            for name, uc in items:
                if uc:
                    errors = sum(1 for q in uc.queries if q.error)
                    table.add_row(
                        name,
                        f"{uc.avg_latency_ms:.1f}",
                        f"{uc.p95_latency_ms:.1f}",
                        f"{uc.total_time_ms:.0f}",
                        str(len(uc.queries)),
                        str(errors) if errors else "-"
                    )
            console.print(table)

        # Cloud cost estimation
        console.print("\n[bold]💰 Cloud Cost Estimate (per month, 10k DERs @ 1s)[/]")
        table = Table()
        table.add_column("Database")
        table.add_column("Storage/mo", justify="right")
        table.add_column("Compute", justify="right")
        table.add_column("Total Est.", justify="right")

        for name, bench in self.results.items():
            if bench.ingestion and not bench.ingestion.error:
                # Extrapolate storage: 5min test → 1 month
                storage_multiplier = (30 * 24 * 60) / self.config.duration_minutes
                monthly_storage_gb = (bench.disk_usage_mb * storage_multiplier) / 1024 if bench.disk_usage_mb else 0
                storage_cost = monthly_storage_gb * 0.10  # ~$0.10/GB/mo for SSD

                # Estimate compute based on CPU/memory
                # Assume we need enough to handle 10k DER ingestion + queries
                cpu_cores = max(2, bench.ingestion.cpu_avg / 25)  # ~25% per core
                memory_gb = bench.ingestion.memory_peak_mb / 1024
                compute_cost = (cpu_cores * 30) + (memory_gb * 5)  # rough estimates

                total = storage_cost + compute_cost

                table.add_row(
                    name,
                    f"${storage_cost:.0f} ({monthly_storage_gb:.0f} GB)",
                    f"${compute_cost:.0f} ({cpu_cores:.1f} CPU, {memory_gb:.1f} GB)",
                    f"${total:.0f}"
                )
        console.print(table)

    def save_results(self):
        """Save results to JSON."""
        os.makedirs("../results", exist_ok=True)

        data = {
            "config": asdict(self.config),
            "timestamp": datetime.now().isoformat(),
            "databases": {}
        }

        for name, bench in self.results.items():
            db_data = {
                "ingestion": asdict(bench.ingestion) if bench.ingestion else None,
                "disk_usage_mb": bench.disk_usage_mb,
                "compression_ratio": bench.compression_ratio,
                "use_cases": {}
            }

            for uc_name, uc in bench.use_cases.items():
                db_data["use_cases"][uc_name] = {
                    "name": uc.name,
                    "avg_latency_ms": uc.avg_latency_ms,
                    "p95_latency_ms": uc.p95_latency_ms,
                    "total_time_ms": uc.total_time_ms,
                    "query_count": len(uc.queries),
                    "error_count": sum(1 for q in uc.queries if q.error)
                }

            data["databases"][name] = db_data

        with open("../results/benchmark_results.json", "w") as f:
            json.dump(data, f, indent=2, default=str)

        console.print(f"\n[green]Results saved to ../results/benchmark_results.json[/]")


async def main():
    console.print(Panel.fit(
        "[bold blue]TSDB Benchmark Suite - Sourceful Energy[/]\n"
        "[dim]10,000 DERs @ 1s interval • Use Case Testing • Resource Monitoring[/]",
        border_style="blue"
    ))

    config = BenchmarkConfig()
    config.print_config()

    runner = BenchmarkRunner(config)
    await runner.run_all()
    runner.print_results()
    runner.save_results()


if __name__ == "__main__":
    asyncio.run(main())
