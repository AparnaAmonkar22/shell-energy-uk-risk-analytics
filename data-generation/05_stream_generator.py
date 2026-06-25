"""
Live streaming event generator for the Shell Energy UK project.

This script simulates the "live API" your Streaming Pipeline (Event Hub +
Stream Analytics) will consume. It does NOT call any real external API -
it generates events in the same shape as the historical data, compressed
in time so a demo can show a full day of HH intervals in a few minutes.

USAGE:
  Dry run (prints JSON to console, no Azure needed - use this first):
    python3 05_stream_generator.py --mode console

  Send to Event Hub (once you have a connection string):
    python3 05_stream_generator.py --mode eventhub \
        --conn-str "<EVENT_HUB_NAMESPACE_CONNECTION_STRING>" \
        --meter-eventhub-name "aparna-meter-readings-stream" \
        --price-eventhub-name "aparna-wholesale-price-stream"

  NOTE: pass the NAMESPACE-level connection string (RootManageSharedAccessKey),
  not a connection string scoped to a single Event Hub - this script sends to
  two separate Event Hubs within the same namespace.

Two event types are interleaved:
  - meter_reading events  (one simulated HH read per active site, per tick)
  - wholesale_price event (one per tick, market-wide)

SIM_SECONDS_PER_INTERVAL controls how fast simulated time moves: with the
default of 5, each 5 real seconds = one 30-minute simulated interval, so a
full day streams in ~4 minutes - good for a live demo.

Install the Event Hub SDK only if you intend to actually send events:
  pip install azure-eventhub --break-system-packages
"""

import argparse
import csv as csv_module
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

random.seed()

SIM_SECONDS_PER_INTERVAL = 5

# Resolve data file paths relative to this script's own location, not a
# hardcoded sandbox path - this lets the script run correctly from Cloud
# Shell, a laptop, or anywhere else once sites.csv/customers.csv sit next
# to it (or inside a dev_subset/ subfolder next to it).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _find_data_file(filename):
    """Looks for filename next to this script, or inside a dev_subset/ subfolder."""
    candidates = [
        os.path.join(SCRIPT_DIR, filename),
        os.path.join(SCRIPT_DIR, "dev_subset", filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Could not find {filename}. Looked in: {candidates}. "
        f"Make sure sites.csv and customers.csv are in the same folder as this script "
        f"(or in a dev_subset/ subfolder next to it)."
    )


def load_site_metadata():
    sites = {}
    with open(_find_data_file("sites.csv")) as f:
        for row in csv_module.DictReader(f):
            sites[row["site_id"]] = {
                "mpan": row["mpan"],
                "customer_id": row["customer_id"],
                "annual_baseline_kwh": float(row["annual_baseline_kwh"]),
            }

    sector_by_customer = {}
    with open(_find_data_file("customers.csv")) as f:
        for row in csv_module.DictReader(f):
            sector_by_customer[row["customer_id"]] = row["sector"]

    for site_id, meta in sites.items():
        meta["sector"] = sector_by_customer.get(meta["customer_id"], "Office / Commercial Real Estate")

    return sites


def sector_load_factor(sector, dt):
    hour = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5
    if sector == "Office / Commercial Real Estate":
        return 0.08 if is_weekend else 0.05 + 0.95 * math.exp(-((hour - 13) ** 2) / (2 * 4.0 ** 2))
    if sector == "Data Centre":
        return 0.92 + 0.08 * math.sin((hour / 24) * 2 * math.pi)
    if sector == "Retail":
        return 0.25 + 0.75 * math.exp(-((hour - 14) ** 2) / (2 * 5.5 ** 2))
    if sector == "Manufacturing":
        return 0.75 if 6 <= hour < 22 and not is_weekend else 0.25
    return 0.5


def gen_meter_event(site_id, meta, sim_time):
    shape = sector_load_factor(meta["sector"], sim_time)
    avg_interval_kwh = meta["annual_baseline_kwh"] / 17520.0
    noise = random.gauss(1.0, 0.08)
    kwh = max(avg_interval_kwh * shape * noise * 2.1, 0)

    meter_status = "Normal"
    if random.random() < 0.002:
        meter_status = "Fault"
        kwh = 0.0

    return {
        "event_type": "meter_reading",
        "site_id": site_id,
        "mpan": meta["mpan"],
        "interval_start": sim_time.isoformat(),
        "kwh_interval": round(kwh, 4),
        "meter_status": meter_status,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def gen_price_event(sim_time):
    hour = sim_time.hour + sim_time.minute / 60.0
    morning = 0.9 * math.exp(-((hour - 8.0) ** 2) / (2 * 2.2 ** 2))
    evening = 1.3 * math.exp(-((hour - 18.0) ** 2) / (2 * 2.5 ** 2))
    shape = 0.35 + morning + evening
    price = max(60.0 * (1 + shape) + random.gauss(0, 5), 5.0)
    return {
        "event_type": "wholesale_price",
        "interval_start": sim_time.isoformat(),
        "price_gbp_per_mwh": round(price, 2),
        "market_period": "Peak" if 7 <= hour < 23 else "Off-Peak",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def run(mode, conn_str=None, meter_eventhub_name=None, price_eventhub_name=None,
        sites_sample=25, start_at=None):
    sites = load_site_metadata()
    site_ids = list(sites.keys())
    sample = random.sample(site_ids, min(sites_sample, len(site_ids)))

    meter_producer = None
    price_producer = None
    if mode == "eventhub":
        from azure.eventhub import EventHubProducerClient, EventData

        meter_producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=meter_eventhub_name
        )
        price_producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=price_eventhub_name
        )

    sim_time = start_at or datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    print(f"Starting stream simulation. {len(sample)} sites, "
          f"{SIM_SECONDS_PER_INTERVAL}s real time = 30 sim minutes. Ctrl+C to stop.",
          file=sys.stderr)

    try:
        while True:
            price_event = gen_price_event(sim_time)
            meter_events = [gen_meter_event(site_id, sites[site_id], sim_time) for site_id in sample]

            if mode == "console":
                print(json.dumps(price_event))
                for e in meter_events:
                    print(json.dumps(e))
            elif mode == "eventhub":
                price_batch = price_producer.create_batch()
                price_batch.add(EventData(json.dumps(price_event)))
                price_producer.send_batch(price_batch)

                meter_batch = meter_producer.create_batch()
                for e in meter_events:
                    meter_batch.add(EventData(json.dumps(e)))
                meter_producer.send_batch(meter_batch)

                print(f"Sent 1 price event + {len(meter_events)} meter events for "
                      f"sim_time={sim_time.isoformat()}", file=sys.stderr)

            sim_time += timedelta(minutes=30)
            time.sleep(SIM_SECONDS_PER_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
    finally:
        if meter_producer:
            meter_producer.close()
        if price_producer:
            price_producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["console", "eventhub"], default="console")
    parser.add_argument("--conn-str", default=None, help="Event Hub NAMESPACE connection string (RootManageSharedAccessKey)")
    parser.add_argument("--meter-eventhub-name", default=None, help="Event Hub name for meter readings, e.g. aparna-meter-readings-stream")
    parser.add_argument("--price-eventhub-name", default=None, help="Event Hub name for wholesale prices, e.g. aparna-wholesale-price-stream")
    parser.add_argument("--sites-sample", type=int, default=25, help="Number of sites to simulate concurrently")
    args = parser.parse_args()

    if args.mode == "eventhub" and (not args.conn_str or not args.meter_eventhub_name or not args.price_eventhub_name):
        print("ERROR: --conn-str, --meter-eventhub-name and --price-eventhub-name are all required for --mode eventhub", file=sys.stderr)
        sys.exit(1)

    run(args.mode, conn_str=args.conn_str, meter_eventhub_name=args.meter_eventhub_name,
        price_eventhub_name=args.price_eventhub_name, sites_sample=args.sites_sample)
