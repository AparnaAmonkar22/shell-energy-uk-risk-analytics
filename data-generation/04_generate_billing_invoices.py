"""
Generates billing_invoices.csv - one row per site per billing month,
2024-01 to 2026-06.

Billed kWh is calculated by SUMMING the actual meter_readings for that site
in that month (so it reconciles with the fact table - this is what makes the
margin/exposure analysis meaningful downstream: billed revenue uses the
contract rate, while "true cost" in the analytics layer will compare the same
volume against the wholesale price series).

Because this rolls up ~10.6M reading rows, we use pandas groupby in chunks
rather than reading everything into memory at once for safety.
"""

import csv
import glob
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd

random.seed(21)

READINGS_DIR = "/home/claude/dataset_gen/historical_meter_readings"
CONTRACTS_CSV = "/home/claude/dataset_gen/contracts.csv"
OUT_CSV = "/home/claude/dataset_gen/billing_invoices.csv"


def load_contracts():
    df = pd.read_csv(CONTRACTS_CSV, parse_dates=["start_date", "end_date"])
    return df


def find_active_contract(contracts_df, site_id, billing_month_start, billing_month_end):
    site_contracts = contracts_df[contracts_df["site_id"] == site_id]
    # contract active for the majority of the billing month: simplest correct case is
    # start_date <= month_end and end_date >= month_start
    overlapping = site_contracts[
        (site_contracts["start_date"] <= billing_month_end)
        & (site_contracts["end_date"] >= billing_month_start)
    ]
    if overlapping.empty:
        return None
    # if multiple (mid-month renewal), pick the one covering more days - simplification: take first
    return overlapping.iloc[0]


def month_file_range():
    months = []
    cursor = date(2024, 1, 1)
    end = date(2026, 6, 1)
    while cursor <= end:
        months.append((cursor.year, cursor.month))
        cursor = date(cursor.year + (cursor.month == 12), (cursor.month % 12) + 1, 1)
    return months


if __name__ == "__main__":
    contracts_df = load_contracts()
    invoices = []
    invoice_counter = 1

    for year, month in month_file_range():
        file_path = os.path.join(READINGS_DIR, f"meter_readings_{year}{month:02d}.csv")
        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path, usecols=["site_id", "kwh_interval"])
        monthly_totals = df.groupby("site_id")["kwh_interval"].sum().reset_index()
        monthly_totals.columns = ["site_id", "total_kwh"]

        month_start = pd.Timestamp(year, month, 1)
        month_end = (month_start + pd.offsets.MonthEnd(1))

        for _, row in monthly_totals.iterrows():
            site_id = row["site_id"]
            billed_kwh = row["total_kwh"]

            contract = find_active_contract(contracts_df, site_id, month_start, month_end)
            if contract is None:
                continue

            rate = contract["rate_gbp_per_kwh"]
            standing_charge_daily = contract["standing_charge_gbp_per_day"]
            days_in_month = (month_end - month_start).days + 1

            energy_charge = billed_kwh * rate
            standing_charge = standing_charge_daily * days_in_month
            billed_amount = round(energy_charge + standing_charge, 2)

            invoice_date = month_end + timedelta(days=random.randint(3, 12))
            due_date = invoice_date + timedelta(days=14)

            # Most invoices paid on time, some late, a few disputed/unpaid (realistic AR aging)
            paid_status = random.choices(
                ["Paid", "Paid Late", "Outstanding", "Disputed"],
                weights=[75, 15, 7, 3],
            )[0]

            invoices.append({
                "invoice_id": f"INV{invoice_counter:06d}",
                "site_id": site_id,
                "contract_id": contract["contract_id"],
                "billing_period_start": month_start.date().isoformat(),
                "billing_period_end": month_end.date().isoformat(),
                "billed_kwh": round(billed_kwh, 2),
                "energy_charge_gbp": round(energy_charge, 2),
                "standing_charge_gbp": round(standing_charge, 2),
                "billed_amount_gbp": billed_amount,
                "invoice_date": invoice_date.date().isoformat(),
                "due_date": due_date.date().isoformat(),
                "paid_status": paid_status,
            })
            invoice_counter += 1

        print(f"{year}-{month:02d}: processed, running total {len(invoices):,} invoices")

    with open(OUT_CSV, "w", newline="") as f:
        fieldnames = list(invoices[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(invoices)

    print(f"\nWrote {len(invoices):,} invoices -> {OUT_CSV}")
