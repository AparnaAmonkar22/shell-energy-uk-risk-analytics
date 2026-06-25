"""
Generates wholesale_prices_historical.csv - a half-hourly (HH) GB wholesale
electricity price series from 2024-01-01 to 2026-06-25.

Shape is modelled (not copied from any real source) on well-known public
characteristics of GB day-ahead power prices:
  - Strong daily double-peak (morning ~08:00, evening ~17:00-19:00)
  - Overnight trough (cheapest ~02:00-04:00)
  - Winter prices materially higher than summer (heating demand)
  - Random day-to-day volatility + occasional price spikes (cold snaps,
    low-wind days) to give the dataset realistic anomalies to detect
"""

import csv
import math
import random
from datetime import datetime, timedelta

random.seed(7)

START = datetime(2024, 1, 1, 0, 0)
END = datetime(2026, 6, 25, 23, 30)
INTERVAL_MINUTES = 30


def seasonal_factor(dt):
    day_of_year = dt.timetuple().tm_yday
    return 1.0 + 0.35 * math.cos(2 * math.pi * (day_of_year - 15) / 365.25)


def daily_shape(hour_float):
    morning = 0.9 * math.exp(-((hour_float - 8.0) ** 2) / (2 * 2.2 ** 2))
    evening = 1.3 * math.exp(-((hour_float - 18.0) ** 2) / (2 * 2.5 ** 2))
    base = 0.35
    return base + morning + evening


def market_period(hour_float):
    return "Peak" if 7 <= hour_float < 23 else "Off-Peak"


def generate():
    rows = []
    price_id = 1
    cursor = START
    spike_cooldown = 0

    while cursor <= END:
        hour_float = cursor.hour + cursor.minute / 60.0
        shape = daily_shape(hour_float)
        season = seasonal_factor(cursor)

        base_price_mwh = 60.0
        price = base_price_mwh * season * (1 + shape)
        noise = random.gauss(0, 4.5)
        price += noise

        if spike_cooldown > 0:
            price *= random.uniform(1.4, 2.2)
            spike_cooldown -= 1
        elif random.random() < 0.0015:
            spike_cooldown = random.randint(2, 6)
            price *= random.uniform(1.6, 2.5)

        price = max(price, 5.0)

        rows.append({
            "price_id": f"WP{price_id:07d}",
            "interval_start": cursor.isoformat(),
            "price_gbp_per_mwh": round(price, 2),
            "market_period": market_period(hour_float),
        })
        price_id += 1
        cursor += timedelta(minutes=INTERVAL_MINUTES)

    return rows


if __name__ == "__main__":
    rows = generate()
    out_path = "/home/claude/dataset_gen/wholesale_prices_historical.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["price_id", "interval_start", "price_gbp_per_mwh", "market_period"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows):,} rows -> {out_path}")
    prices = [r["price_gbp_per_mwh"] for r in rows]
    print(f"Price range: min={min(prices):.2f} max={max(prices):.2f} avg={sum(prices)/len(prices):.2f} GBP/MWh")
