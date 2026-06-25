"""
Generates meter_readings_historical.csv - half-hourly (HH) electricity
consumption per site from 2024-01-01 to 2026-06-25.

This is the largest table (~246 sites x ~900 days x 48 intervals/day ~= 10.6M rows).
Consumption shape is sector-aware:
  - Office / Commercial: weekday 08:00-18:00 peak, near-zero overnight/weekends
  - Retail: longer trading hours incl. weekends, evening tail
  - Manufacturing: 2-shift or 3-shift pattern depending on site
  - Data Centre: near-flat 24/7 load (small daily variation only)
  - Logistics & Warehousing: early morning + daytime peaks (loading docks)
  - Hospitality: evening/weekend-weighted
  - Healthcare: flat, slightly elevated daytime
  - Education: term-time weekday daytime only, near-zero in school holidays
  - Food & Beverage Production: continuous with maintenance dips

Data quality issues are injected deliberately so the pipeline has real problems
to solve downstream (handling nulls, flatlines, late-arriving corrections):
  - ~0.3% of intervals: missing (meter comms failure) -> row omitted, to be
    handled by Silver-layer gap-fill / interpolation logic
  - ~0.15% of intervals: meter_status = 'Estimated' (flagged but value present)
  - Occasional short outage runs (site goes to 0 for a few hours - real fault)

Output is written in monthly CSV chunks under historical_meter_readings/ to
keep individual files at a manageable size for ADF/ADLS upload and Databricks
Auto Loader testing.
"""

import csv
import math
import os
import random
from datetime import datetime, timedelta

import pandas as pd

random.seed(11)

SITES_CSV = "/home/claude/dataset_gen/sites.csv"
OUT_DIR = "/home/claude/dataset_gen/historical_meter_readings"
START = datetime(2024, 1, 1, 0, 0)
END = datetime(2026, 6, 25, 23, 30)
INTERVAL_MINUTES = 30

SCHOOL_HOLIDAY_RANGES = [
    (datetime(2024, 7, 20), datetime(2024, 9, 1)),
    (datetime(2024, 12, 20), datetime(2025, 1, 5)),
    (datetime(2025, 7, 20), datetime(2025, 9, 1)),
    (datetime(2025, 12, 20), datetime(2026, 1, 5)),
    (datetime(2026, 4, 1), datetime(2026, 4, 14)),
]


def is_school_holiday(dt):
    return any(s <= dt <= e for s, e in SCHOOL_HOLIDAY_RANGES)


def sector_load_factor(sector, dt):
    """Returns a 0..1+ multiplier representing demand shape at this timestamp."""
    hour = dt.hour + dt.minute / 60.0
    weekday = dt.weekday()  # 0=Mon
    is_weekend = weekday >= 5

    if sector == "Office / Commercial Real Estate":
        if is_weekend:
            return 0.08
        # bell curve 08:00-18:00
        return 0.05 + 0.95 * math.exp(-((hour - 13) ** 2) / (2 * 4.0 ** 2))

    if sector == "Retail":
        if hour < 7 or hour > 22:
            return 0.10
        return 0.25 + 0.75 * math.exp(-((hour - 14) ** 2) / (2 * 5.5 ** 2))

    if sector == "Manufacturing":
        # two-shift: 06:00-14:00 and 14:00-22:00, lighter overnight
        if is_weekend:
            return 0.20
        if 6 <= hour < 22:
            return 0.75 + 0.25 * math.sin((hour - 6) / 16 * math.pi)
        return 0.30

    if sector == "Data Centre":
        # near-flat, small daily ripple from cooling load
        return 0.92 + 0.08 * math.sin((hour / 24) * 2 * math.pi)

    if sector == "Logistics & Warehousing":
        if is_weekend:
            return 0.35
        morning = math.exp(-((hour - 6) ** 2) / (2 * 1.5 ** 2))
        daytime = math.exp(-((hour - 13) ** 2) / (2 * 4 ** 2))
        return 0.20 + 0.5 * morning + 0.5 * daytime

    if sector == "Hospitality":
        evening = math.exp(-((hour - 19) ** 2) / (2 * 3 ** 2))
        lunch = math.exp(-((hour - 12) ** 2) / (2 * 1.5 ** 2))
        weekend_boost = 1.3 if is_weekend else 1.0
        return (0.25 + 0.5 * evening + 0.3 * lunch) * weekend_boost

    if sector == "Healthcare":
        return 0.75 + 0.20 * math.exp(-((hour - 13) ** 2) / (2 * 5 ** 2))

    if sector == "Education":
        if is_weekend or is_school_holiday(dt):
            return 0.10
        return 0.10 + 0.85 * math.exp(-((hour - 12) ** 2) / (2 * 4 ** 2))

    if sector == "Food & Beverage Production":
        base = 0.65 + 0.30 * math.sin((hour / 24) * 2 * math.pi + 1)
        if weekday == 6:  # Sunday maintenance dip
            base *= 0.55
        return base

    return 0.5  # fallback


def seasonal_demand_factor(dt, sector):
    """Heating/cooling seasonal swing layered on top of the daily shape."""
    day_of_year = dt.timetuple().tm_yday
    winter_peak = math.cos(2 * math.pi * (day_of_year - 15) / 365.25)  # +1 in Jan, -1 in Jul
    if sector == "Data Centre":
        return 1.0 + 0.04 * winter_peak  # cooling load barely seasonal, mostly load-driven
    return 1.0 + 0.18 * winter_peak


def load_sites():
    df = pd.read_csv(SITES_CSV)
    return df.to_dict("records")


def load_customer_sectors():
    cust = pd.read_csv("/home/claude/dataset_gen/customers.csv")
    return dict(zip(cust["customer_id"], cust["sector"]))


def generate_month(sites_with_sector, year, month):
    month_start = datetime(year, month, 1)
    next_month = datetime(year + (month == 12), (month % 12) + 1, 1)
    cursor_start = max(month_start, START)
    cursor_end = min(next_month - timedelta(minutes=INTERVAL_MINUTES), END)
    if cursor_start > cursor_end:
        return []

    rows = []
    for site in sites_with_sector:
        sector = site["sector"]
        annual_kwh = site["annual_baseline_kwh"]
        # Convert annual baseline into an average per-interval kWh anchor (17520 HH intervals/year)
        avg_interval_kwh = annual_kwh / 17520.0

        # Per-site outage simulation state
        in_outage = False
        outage_remaining = 0

        cursor = cursor_start
        while cursor <= cursor_end:
            shape = sector_load_factor(sector, cursor)
            season = seasonal_demand_factor(cursor, sector)
            noise = random.gauss(1.0, 0.06)
            kwh = max(avg_interval_kwh * shape * season * noise * 2.1, 0)  # *2.1 normalises shape avg ~0.5 back to baseline

            meter_status = "Normal"
            data_quality_flag = "OK"

            # Outage logic: small chance to start an outage, lasting 2-10 intervals (1-5 hrs)
            if in_outage:
                kwh = 0.0
                meter_status = "Fault"
                data_quality_flag = "OUTAGE"
                outage_remaining -= 1
                if outage_remaining <= 0:
                    in_outage = False
            elif random.random() < 0.00008:
                in_outage = True
                outage_remaining = random.randint(2, 10)
                kwh = 0.0
                meter_status = "Fault"
                data_quality_flag = "OUTAGE"
            elif random.random() < 0.0015:
                # Estimated read (e.g. comms drop, supplier estimates from profile)
                meter_status = "Estimated"
                data_quality_flag = "ESTIMATED"
                kwh *= random.uniform(0.85, 1.15)

            # Missing interval simulation: skip writing the row entirely (~0.3%)
            if random.random() < 0.003:
                cursor += timedelta(minutes=INTERVAL_MINUTES)
                continue

            rows.append((
                site["site_id"],
                cursor.isoformat(),
                round(kwh, 4),
                meter_status,
                data_quality_flag,
            ))
            cursor += timedelta(minutes=INTERVAL_MINUTES)

    return rows


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    sites = load_sites()
    sector_map = load_customer_sectors()
    for s in sites:
        s["sector"] = sector_map.get(s["customer_id"], "Office / Commercial Real Estate")

    total_rows = 0
    reading_id_counter = 1

    cursor_month = datetime(START.year, START.month, 1)
    end_month_marker = datetime(END.year, END.month, 1)

    while cursor_month <= end_month_marker:
        y, m = cursor_month.year, cursor_month.month
        rows = generate_month(sites, y, m)

        out_path = os.path.join(OUT_DIR, f"meter_readings_{y}{m:02d}.csv")
        with open(out_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["reading_id", "site_id", "interval_start", "kwh_interval", "meter_status", "data_quality_flag"])
            for site_id, interval_start, kwh, status, flag in rows:
                writer.writerow([f"MR{reading_id_counter:09d}", site_id, interval_start, kwh, status, flag])
                reading_id_counter += 1

        total_rows += len(rows)
        print(f"{y}-{m:02d}: {len(rows):,} rows -> {out_path}")

        cursor_month = datetime(y + (m == 12), (m % 12) + 1, 1)

    print(f"\nTOTAL: {total_rows:,} meter reading rows across {len(sites)} sites")
