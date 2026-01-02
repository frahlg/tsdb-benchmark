"""
Data Generator for Sourceful Energy TSDB Benchmarks

Generates realistic time-series data matching actual Sourceful DER formats.
Target: 10,000 DERs @ 1s interval = 10M datapoints/hour
"""

import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generator, Literal, Any
import json

import numpy as np


# =============================================================================
# Actual Sourceful Data Models
# =============================================================================

@dataclass
class PVReading:
    """
    PV inverter reading - matches actual Sourceful format.

    Example:
    {
      "W": 0,
      "make": "SolarEdge",
      "mppt1_A": 11597,  # mA (milliamps)
      "mppt1_V": 21586,  # mV (millivolts)
      "mppt2_A": 28530,
      "mppt2_V": 29696,
      "rated_power_W": 8000,
      "read_time_ms": 249,
      "timestamp": "2026-01-01T07:19:10.979000Z",
      "total_generation_Wh": 44981956,
      "type": "pv"
    }
    """
    timestamp: str  # ISO format
    type: str = "pv"
    make: str = "SolarEdge"
    W: int = 0  # Current power in watts
    rated_power_W: int = 8000
    mppt1_V: int = 0  # millivolts
    mppt1_A: int = 0  # milliamps
    mppt2_V: int = 0  # millivolts
    mppt2_A: int = 0  # milliamps
    total_generation_Wh: int = 0
    read_time_ms: int = 100

    # Metadata (for tagging)
    der_id: str = ""
    site_id: str = ""
    device_id: str = ""
    wallet_id: str = ""


@dataclass
class MeterReading:
    """
    Smart meter reading - matches actual Sourceful P1/Zap format.

    Example:
    {
      "L1_A": 0.1,
      "L1_V": 231.7,
      "L1_W": 22,
      "L2_A": 0.7,
      "L2_V": 231.5,
      "L2_W": 154,
      "L3_A": 0.1,
      "L3_V": 232.5,
      "L3_W": 8,
      "W": 185,
      "make": "sourceful zap p1",
      "timestamp": "2026-01-01T07:19:31.727000Z",
      "total_export_Wh": 0,
      "total_import_Wh": 20205362,
      "type": "Meter"
    }
    """
    timestamp: str
    type: str = "Meter"
    make: str = "sourceful zap p1"
    W: int = 0  # Total power
    L1_V: float = 230.0
    L1_A: float = 0.0
    L1_W: int = 0
    L2_V: float = 230.0
    L2_A: float = 0.0
    L2_W: int = 0
    L3_V: float = 230.0
    L3_A: float = 0.0
    L3_W: int = 0
    total_import_Wh: int = 0
    total_export_Wh: int = 0

    # Metadata
    der_id: str = ""
    site_id: str = ""
    device_id: str = ""
    wallet_id: str = ""


@dataclass
class BatteryReading:
    """Battery storage reading."""
    timestamp: str
    type: str = "battery"
    make: str = "BYD"
    W: int = 0  # Positive = charging, negative = discharging
    soc_percent: float = 50.0  # State of charge 0-100
    voltage_V: float = 48.0
    current_A: float = 0.0
    temperature_C: float = 25.0
    total_charged_Wh: int = 0
    total_discharged_Wh: int = 0

    # Metadata
    der_id: str = ""
    site_id: str = ""
    device_id: str = ""
    wallet_id: str = ""


@dataclass
class EVChargerReading:
    """EV charger reading."""
    timestamp: str
    type: str = "ev_charger"
    make: str = "Zaptec"
    W: int = 0
    voltage_V: float = 230.0
    current_A: float = 0.0
    session_Wh: int = 0
    total_Wh: int = 0
    vehicle_connected: bool = False
    vehicle_soc_percent: float | None = None

    # Metadata
    der_id: str = ""
    site_id: str = ""
    device_id: str = ""
    wallet_id: str = ""


# Type alias for any reading
DERReading = PVReading | MeterReading | BatteryReading | EVChargerReading


@dataclass
class DERConfig:
    """Configuration for a Distributed Energy Resource."""
    der_id: str
    der_type: Literal["pv", "Meter", "battery", "ev_charger"]
    make: str
    site_id: str
    device_id: str
    wallet_id: str
    rated_power_W: int = 8000  # For PV/battery


class SourcefulDataGenerator:
    """
    Generates realistic energy data matching Sourceful's actual format.

    Target scale: 10,000 DERs @ 1 second interval
    = 10,000 datapoints/second
    = 600,000 datapoints/minute
    = 36,000,000 datapoints/hour
    """

    # Realistic makes per DER type
    PV_MAKES = ["SolarEdge", "Huawei", "Fronius", "SMA", "Growatt", "GoodWe", "Sungrow"]
    METER_MAKES = ["sourceful zap p1", "Kaifa", "Aidon", "Kamstrup"]
    BATTERY_MAKES = ["BYD", "Tesla Powerwall", "LG Chem", "Sonnen"]
    EV_MAKES = ["Zaptec", "Easee", "Wallbox", "Tesla Wall Connector"]

    def __init__(
        self,
        num_wallets: int = 1000,
        sites_per_wallet: int = 2,
        devices_per_site: int = 1,
        ders_per_device: int = 5,  # Mix of PV, meter, battery, EV
        seed: int = 42
    ):
        self.num_wallets = num_wallets
        self.sites_per_wallet = sites_per_wallet
        self.devices_per_site = devices_per_site
        self.ders_per_device = ders_per_device

        random.seed(seed)
        np.random.seed(seed)

        # State tracking for cumulative values (must be before _generate_topology)
        self._cumulative_state: dict[str, dict] = {}

        self.ders = self._generate_topology()

    def _generate_topology(self) -> list[DERConfig]:
        """Generate the WALLET → SITE → DEVICE → DER hierarchy."""
        ders = []

        # Distribution: 40% meters, 35% PV, 15% battery, 10% EV
        der_type_distribution = ["Meter", "Meter", "pv", "pv", "battery", "pv", "Meter", "pv", "ev_charger", "Meter"]

        for w in range(self.num_wallets):
            wallet_id = f"wallet_{w:05d}"

            for s in range(self.sites_per_wallet):
                site_id = f"site_{w:05d}_{s:02d}"

                for d in range(self.devices_per_site):
                    device_id = f"device_{w:05d}_{s:02d}_{d:02d}"

                    for der_idx in range(self.ders_per_device):
                        der_type = der_type_distribution[der_idx % len(der_type_distribution)]
                        der_id = f"der_{w:05d}_{s:02d}_{d:02d}_{der_idx:02d}"

                        # Select make based on type
                        if der_type == "pv":
                            make = random.choice(self.PV_MAKES)
                            rated_power = random.choice([3000, 5000, 6000, 8000, 10000, 12000])
                        elif der_type == "Meter":
                            make = random.choice(self.METER_MAKES)
                            rated_power = 0
                        elif der_type == "battery":
                            make = random.choice(self.BATTERY_MAKES)
                            rated_power = random.choice([3300, 5000, 7000, 10000, 13500])
                        else:  # ev_charger
                            make = random.choice(self.EV_MAKES)
                            rated_power = random.choice([3700, 7400, 11000, 22000])

                        ders.append(DERConfig(
                            der_id=der_id,
                            der_type=der_type,
                            make=make,
                            site_id=site_id,
                            device_id=device_id,
                            wallet_id=wallet_id,
                            rated_power_W=rated_power
                        ))

                        # Initialize cumulative state
                        self._cumulative_state[der_id] = {
                            "total_generation_Wh": random.randint(10000000, 100000000),
                            "total_import_Wh": random.randint(5000000, 50000000),
                            "total_export_Wh": random.randint(0, 10000000),
                            "total_charged_Wh": random.randint(100000, 5000000),
                            "total_discharged_Wh": random.randint(100000, 5000000),
                            "total_ev_Wh": random.randint(10000, 1000000),
                            "soc": random.uniform(20, 80),
                        }

        return ders

    @property
    def total_ders(self) -> int:
        return len(self.ders)

    def _get_solar_factor(self, hour: int, minute: int = 0) -> float:
        """Get solar production factor based on time of day."""
        # Solar curve: sunrise ~6, peak ~12, sunset ~20
        if hour < 5 or hour > 21:
            return 0.0

        # Use sine curve for realistic solar pattern
        hour_decimal = hour + minute / 60.0
        if 5 <= hour_decimal <= 21:
            # Peak at 13:00 (solar noon accounting for DST)
            factor = np.sin(np.pi * (hour_decimal - 5) / 16)
            return max(0, factor)
        return 0.0

    def _generate_pv_reading(self, der: DERConfig, ts: datetime) -> PVReading:
        """Generate PV inverter reading."""
        state = self._cumulative_state[der.der_id]
        hour = ts.hour
        minute = ts.minute

        solar_factor = self._get_solar_factor(hour, minute)

        # Add cloud/weather variance
        weather_factor = 0.7 + 0.3 * random.random()

        # Calculate power
        base_power = der.rated_power_W * solar_factor * weather_factor
        power_w = int(max(0, base_power + np.random.normal(0, base_power * 0.05)))

        # MPPT values (in millivolts/milliamps)
        if power_w > 0:
            mppt1_v = random.randint(280000, 400000)  # 280-400V in mV
            mppt1_a = int((power_w * 0.5) / (mppt1_v / 1000) * 1000)  # Convert to mA
            mppt2_v = random.randint(280000, 400000)
            mppt2_a = int((power_w * 0.5) / (mppt2_v / 1000) * 1000)
        else:
            mppt1_v = mppt1_a = mppt2_v = mppt2_a = 0

        # Update cumulative
        energy_wh = power_w / 3600  # 1 second interval
        state["total_generation_Wh"] += int(energy_wh)

        return PVReading(
            timestamp=ts.isoformat() + "Z",
            make=der.make,
            W=power_w,
            rated_power_W=der.rated_power_W,
            mppt1_V=mppt1_v,
            mppt1_A=mppt1_a,
            mppt2_V=mppt2_v,
            mppt2_A=mppt2_a,
            total_generation_Wh=state["total_generation_Wh"],
            read_time_ms=random.randint(50, 500),
            der_id=der.der_id,
            site_id=der.site_id,
            device_id=der.device_id,
            wallet_id=der.wallet_id
        )

    def _generate_meter_reading(self, der: DERConfig, ts: datetime) -> MeterReading:
        """Generate smart meter reading with 3-phase data."""
        state = self._cumulative_state[der.der_id]
        hour = ts.hour

        # Consumption pattern
        if 7 <= hour <= 9:  # Morning peak
            base_load = random.randint(2000, 5000)
        elif 17 <= hour <= 21:  # Evening peak
            base_load = random.randint(3000, 8000)
        elif 0 <= hour <= 6:  # Night
            base_load = random.randint(100, 500)
        else:  # Day
            base_load = random.randint(500, 2000)

        # Distribute across phases (with realistic imbalance)
        phase_weights = np.random.dirichlet([2, 2, 2])

        l1_w = int(base_load * phase_weights[0])
        l2_w = int(base_load * phase_weights[1])
        l3_w = int(base_load * phase_weights[2])
        total_w = l1_w + l2_w + l3_w

        # Voltages (Swedish grid ~230V, slight variance)
        l1_v = round(230 + random.uniform(-3, 3), 1)
        l2_v = round(230 + random.uniform(-3, 3), 1)
        l3_v = round(230 + random.uniform(-3, 3), 1)

        # Currents
        l1_a = round(l1_w / l1_v, 1) if l1_v > 0 else 0
        l2_a = round(l2_w / l2_v, 1) if l2_v > 0 else 0
        l3_a = round(l3_w / l3_v, 1) if l3_v > 0 else 0

        # Update cumulative (1 second worth of energy)
        state["total_import_Wh"] += int(total_w / 3600)

        return MeterReading(
            timestamp=ts.isoformat() + "Z",
            make=der.make,
            W=total_w,
            L1_V=l1_v,
            L1_A=l1_a,
            L1_W=l1_w,
            L2_V=l2_v,
            L2_A=l2_a,
            L2_W=l2_w,
            L3_V=l3_v,
            L3_A=l3_a,
            L3_W=l3_w,
            total_import_Wh=state["total_import_Wh"],
            total_export_Wh=state["total_export_Wh"],
            der_id=der.der_id,
            site_id=der.site_id,
            device_id=der.device_id,
            wallet_id=der.wallet_id
        )

    def _generate_battery_reading(self, der: DERConfig, ts: datetime) -> BatteryReading:
        """Generate battery storage reading."""
        state = self._cumulative_state[der.der_id]
        hour = ts.hour

        # Battery behavior: charge during solar peak, discharge evening
        if 10 <= hour <= 15:  # Charging period
            power = random.randint(1000, min(3000, der.rated_power_W))
            state["soc"] = min(100, state["soc"] + 0.01)
            state["total_charged_Wh"] += int(power / 3600)
        elif 17 <= hour <= 22:  # Discharge period
            power = -random.randint(1000, min(3000, der.rated_power_W))
            state["soc"] = max(10, state["soc"] - 0.01)
            state["total_discharged_Wh"] += int(abs(power) / 3600)
        else:
            power = random.randint(-200, 200)  # Idle/standby

        return BatteryReading(
            timestamp=ts.isoformat() + "Z",
            make=der.make,
            W=power,
            soc_percent=round(state["soc"], 1),
            voltage_V=round(48 + random.uniform(-2, 4), 1),
            current_A=round(abs(power) / 48, 1),
            temperature_C=round(25 + random.uniform(-5, 10), 1),
            total_charged_Wh=state["total_charged_Wh"],
            total_discharged_Wh=state["total_discharged_Wh"],
            der_id=der.der_id,
            site_id=der.site_id,
            device_id=der.device_id,
            wallet_id=der.wallet_id
        )

    def _generate_ev_reading(self, der: DERConfig, ts: datetime) -> EVChargerReading:
        """Generate EV charger reading."""
        state = self._cumulative_state[der.der_id]
        hour = ts.hour

        # EV charging: mostly overnight and some daytime
        is_charging = (22 <= hour or hour <= 7) and random.random() > 0.3
        is_charging = is_charging or (9 <= hour <= 16 and random.random() > 0.8)

        if is_charging:
            power = der.rated_power_W
            current = power / 230
            vehicle_soc = random.uniform(20, 90)
            state["total_ev_Wh"] += int(power / 3600)
        else:
            power = 0
            current = 0
            vehicle_soc = None

        return EVChargerReading(
            timestamp=ts.isoformat() + "Z",
            make=der.make,
            W=power,
            voltage_V=round(230 + random.uniform(-2, 2), 1) if power > 0 else 0,
            current_A=round(current, 1),
            session_Wh=random.randint(0, 50000) if power > 0 else 0,
            total_Wh=state["total_ev_Wh"],
            vehicle_connected=power > 0,
            vehicle_soc_percent=vehicle_soc,
            der_id=der.der_id,
            site_id=der.site_id,
            device_id=der.device_id,
            wallet_id=der.wallet_id
        )

    def generate_reading(self, der: DERConfig, ts: datetime) -> DERReading:
        """Generate a single reading for a DER."""
        generators = {
            "pv": self._generate_pv_reading,
            "Meter": self._generate_meter_reading,
            "battery": self._generate_battery_reading,
            "ev_charger": self._generate_ev_reading,
        }
        return generators[der.der_type](der, ts)

    def generate_batch(
        self,
        start_time: datetime,
        duration_seconds: int = 3600,
        interval_seconds: int = 1,
    ) -> Generator[DERReading, None, None]:
        """
        Generate batch of readings for all DERs.

        Args:
            start_time: Start timestamp
            duration_seconds: Duration in seconds
            interval_seconds: Interval between readings

        Yields:
            DERReading for each DER at each timestamp
        """
        num_intervals = duration_seconds // interval_seconds

        for i in range(num_intervals):
            ts = start_time + timedelta(seconds=i * interval_seconds)
            for der in self.ders:
                yield self.generate_reading(der, ts)

    def to_influx_line_protocol(self, reading: DERReading) -> str:
        """Convert reading to InfluxDB Line Protocol."""
        # Parse timestamp
        ts_str = reading.timestamp.rstrip("Z")
        dt = datetime.fromisoformat(ts_str)
        ts_ns = int(dt.timestamp() * 1_000_000_000)

        # Tags
        tags = f"der_id={reading.der_id},der_type={reading.type},site_id={reading.site_id},device_id={reading.device_id},wallet_id={reading.wallet_id},make={reading.make.replace(' ', '_')}"

        # Fields based on type
        if isinstance(reading, PVReading):
            fields = f"W={reading.W}i,rated_power_W={reading.rated_power_W}i,mppt1_V={reading.mppt1_V}i,mppt1_A={reading.mppt1_A}i,mppt2_V={reading.mppt2_V}i,mppt2_A={reading.mppt2_A}i,total_generation_Wh={reading.total_generation_Wh}i,read_time_ms={reading.read_time_ms}i"
        elif isinstance(reading, MeterReading):
            fields = f"W={reading.W}i,L1_V={reading.L1_V},L1_A={reading.L1_A},L1_W={reading.L1_W}i,L2_V={reading.L2_V},L2_A={reading.L2_A},L2_W={reading.L2_W}i,L3_V={reading.L3_V},L3_A={reading.L3_A},L3_W={reading.L3_W}i,total_import_Wh={reading.total_import_Wh}i,total_export_Wh={reading.total_export_Wh}i"
        elif isinstance(reading, BatteryReading):
            fields = f"W={reading.W}i,soc_percent={reading.soc_percent},voltage_V={reading.voltage_V},current_A={reading.current_A},temperature_C={reading.temperature_C},total_charged_Wh={reading.total_charged_Wh}i,total_discharged_Wh={reading.total_discharged_Wh}i"
        else:  # EVChargerReading
            fields = f"W={reading.W}i,voltage_V={reading.voltage_V},current_A={reading.current_A},session_Wh={reading.session_Wh}i,total_Wh={reading.total_Wh}i,vehicle_connected={str(reading.vehicle_connected).lower()}"
            if reading.vehicle_soc_percent is not None:
                fields += f",vehicle_soc_percent={reading.vehicle_soc_percent}"

        return f"energy,{tags} {fields} {ts_ns}"

    def to_json(self, reading: DERReading) -> str:
        """Convert reading to JSON string."""
        # Get all fields from dataclass
        data = {}
        for field_name in reading.__dataclass_fields__:
            value = getattr(reading, field_name)
            if value is not None:
                data[field_name] = value
        return json.dumps(data)

    def to_dict(self, reading: DERReading) -> dict[str, Any]:
        """Convert reading to dictionary."""
        data = {}
        for field_name in reading.__dataclass_fields__:
            value = getattr(reading, field_name)
            if value is not None:
                data[field_name] = value
        return data


def main():
    """Demo: Generate sample data at scale."""
    print("Sourceful Energy Data Generator")
    print("=" * 50)

    # Target: 10,000 DERs
    # 2000 wallets × 2 sites × 1 device × 5 DERs = 20,000 DERs (we'll use 1000 wallets)
    gen = SourcefulDataGenerator(
        num_wallets=1000,
        sites_per_wallet=2,
        devices_per_site=1,
        ders_per_device=5
    )

    print(f"\nTopology:")
    print(f"  Wallets:      {gen.num_wallets:,}")
    print(f"  Sites:        {gen.num_wallets * gen.sites_per_wallet:,}")
    print(f"  Total DERs:   {gen.total_ders:,}")
    print(f"\nAt 1s interval:")
    print(f"  Datapoints/sec:  {gen.total_ders:,}")
    print(f"  Datapoints/min:  {gen.total_ders * 60:,}")
    print(f"  Datapoints/hour: {gen.total_ders * 3600:,}")

    # Generate sample data
    start = datetime(2026, 1, 1, 12, 0, 0)

    print("\nSample PV reading:")
    pv_ders = [d for d in gen.ders if d.der_type == "pv"][:1]
    for der in pv_ders:
        reading = gen.generate_reading(der, start)
        print(gen.to_json(reading))

    print("\nSample Meter reading:")
    meter_ders = [d for d in gen.ders if d.der_type == "Meter"][:1]
    for der in meter_ders:
        reading = gen.generate_reading(der, start)
        print(gen.to_json(reading))

    print("\nInflux Line Protocol sample:")
    for der in gen.ders[:2]:
        reading = gen.generate_reading(der, start)
        print(gen.to_influx_line_protocol(reading))


if __name__ == "__main__":
    main()
