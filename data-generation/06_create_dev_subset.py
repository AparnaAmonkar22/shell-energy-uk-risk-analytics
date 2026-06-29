"""
Creates a small dev/test subset from the already-generated full dataset:
  - 15 sites (instead of 246) - reduced from an earlier 30-site version
    specifically to keep volume modest now that the full 2024-2026 date
    range is included, since site count and date range both multiply
    total row count
  - FULL 30-month range (2024-01 through 2026-06) instead of just the most
    recent 6 months - needed so dashboards can show genuine multi-year
    trend comparison, not just a single half-year snapshot
  - matching subset of customers, contracts, billing_invoices
  - wholesale_prices_historical filtered to the same full date window
    (this one stays site-independent so it's just a date filter)

Sized to land around ~35-40MB total for meter readings, comfortably within
GitHub's normal file-size comfort zone, so this can still be pushed to the
repo and pulled by ADF via the existing HTTP connector pattern - no need to
switch to a direct ADLS upload for this volume.

This does NOT touch the full dataset - it reads from it and writes a
separate dev_subset/ folder, so you can point ADF/Databricks at whichever
one suits the stage you're at.
"""

import os
import shutil

import pandas as pd

BASE = "/home/claude/dataset_gen"
DEV_DIR = os.path.join(BASE, "dev_subset")
NUM_DEV_SITES = 10
DEV_MONTHS = [
    "202401", "202402", "202403", "202404", "202405", "202406",
    "202407", "202408", "202409", "202410", "202411", "202412",
    "202501", "202502", "202503", "202504", "202505", "202506",
    "202507", "202508", "202509", "202510", "202511", "202512",
    "202601", "202602", "202603", "202604", "202605", "202606",
]  # full 2024-01 through 2026-06 range, 30 months total

os.makedirs(DEV_DIR, exist_ok=True)
os.makedirs(os.path.join(DEV_DIR, "historical_meter_readings"), exist_ok=True)

# 1. Pick 15 sites, but bias selection to include a mix of sectors so the
#    dev set still demonstrates the different consumption shapes
sites = pd.read_csv(os.path.join(BASE, "sites.csv"))
customers = pd.read_csv(os.path.join(BASE, "customers.csv"))
sites_with_sector = sites.merge(customers[["customer_id", "sector"]], on="customer_id")

dev_sites = (
    sites_with_sector.groupby("sector", group_keys=False)
    .apply(lambda g: g.sample(min(len(g), max(1, NUM_DEV_SITES // sites_with_sector["sector"].nunique())), random_state=1))
)
# Top up / trim to exactly NUM_DEV_SITES
if len(dev_sites) > NUM_DEV_SITES:
    dev_sites = dev_sites.sample(NUM_DEV_SITES, random_state=1)
elif len(dev_sites) < NUM_DEV_SITES:
    remaining = sites_with_sector[~sites_with_sector["site_id"].isin(dev_sites["site_id"])]
    topup = remaining.sample(NUM_DEV_SITES - len(dev_sites), random_state=1)
    dev_sites = pd.concat([dev_sites, topup])

dev_site_ids = set(dev_sites["site_id"])
dev_customer_ids = set(dev_sites["customer_id"])

# Write sites.csv (drop the helper sector column to match original schema)
dev_sites_out = sites[sites["site_id"].isin(dev_site_ids)]
dev_sites_out.to_csv(os.path.join(DEV_DIR, "sites.csv"), index=False)

# 2. Customers - only those referenced by dev sites
dev_customers = customers[customers["customer_id"].isin(dev_customer_ids)]
dev_customers.to_csv(os.path.join(DEV_DIR, "customers.csv"), index=False)

# 3. Contracts - only for dev sites
contracts = pd.read_csv(os.path.join(BASE, "contracts.csv"))
dev_contracts = contracts[contracts["site_id"].isin(dev_site_ids)]
dev_contracts.to_csv(os.path.join(DEV_DIR, "contracts.csv"), index=False)

# 4. Billing invoices - only for dev sites AND within the dev month window
billing = pd.read_csv(os.path.join(BASE, "billing_invoices.csv"))
billing["period_key"] = pd.to_datetime(billing["billing_period_start"]).dt.strftime("%Y%m")
dev_billing = billing[
    billing["site_id"].isin(dev_site_ids) & billing["period_key"].isin(DEV_MONTHS)
].drop(columns=["period_key"])
dev_billing.to_csv(os.path.join(DEV_DIR, "billing_invoices.csv"), index=False)

# 5. Meter readings - filter each of the 6 monthly files down to dev sites only
total_dev_reading_rows = 0
for month_key in DEV_MONTHS:
    src = os.path.join(BASE, "historical_meter_readings", f"meter_readings_{month_key}.csv")
    if not os.path.exists(src):
        print(f"WARNING: {src} not found, skipping")
        continue
    df = pd.read_csv(src)
    dev_df = df[df["site_id"].isin(dev_site_ids)]
    dst = os.path.join(DEV_DIR, "historical_meter_readings", f"meter_readings_{month_key}.csv")
    dev_df.to_csv(dst, index=False)
    total_dev_reading_rows += len(dev_df)
    print(f"{month_key}: {len(dev_df):,} rows -> {dst}")

# 6. Wholesale prices - filter to the same 6-month date window (site-independent)
prices = pd.read_csv(os.path.join(BASE, "wholesale_prices_historical.csv"), parse_dates=["interval_start"])
month_starts = pd.to_datetime(DEV_MONTHS, format="%Y%m")
window_start = month_starts.min()
window_end = (month_starts.max() + pd.offsets.MonthEnd(1))
dev_prices = prices[(prices["interval_start"] >= window_start) & (prices["interval_start"] <= window_end)]
dev_prices.to_csv(os.path.join(DEV_DIR, "wholesale_prices_historical.csv"), index=False)

print(f"\n=== DEV SUBSET SUMMARY ===")
print(f"Sites: {len(dev_sites_out)}  Customers: {len(dev_customers)}  Contracts: {len(dev_contracts)}")
print(f"Billing invoices: {len(dev_billing)}")
print(f"Meter readings: {total_dev_reading_rows:,} rows across {len(DEV_MONTHS)} months")
print(f"Wholesale prices: {len(dev_prices):,} rows")
print(f"Output dir: {DEV_DIR}")
