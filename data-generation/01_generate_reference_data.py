"""
Generates reference (dimension) data for the Shell Energy UK C&I analytics project:
  - customers.csv
  - sites.csv
  - contracts.csv

These are the "slow changing" entities that the batch and streaming fact data
will reference via site_id / mpan. Run this FIRST - everything else depends on it.
"""

import csv
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker("en_GB")
random.seed(42)
Faker.seed(42)

NUM_CUSTOMERS = 150
SECTORS = [
    "Retail", "Manufacturing", "Office / Commercial Real Estate",
    "Data Centre", "Logistics & Warehousing", "Hospitality",
    "Healthcare", "Education", "Food & Beverage Production"
]
# London-weighted regions but with realistic GB spread (a C&I supplier isn't only London)
REGIONS = [
    ("London", 0.40), ("South East", 0.15), ("North West", 0.10),
    ("West Midlands", 0.08), ("Yorkshire and the Humber", 0.07),
    ("East of England", 0.07), ("Scotland", 0.06), ("South West", 0.04),
    ("North East", 0.03),
]

LONDON_POSTCODE_PREFIXES = ["EC1", "EC2", "EC3", "EC4", "WC1", "WC2", "SE1", "SW1", "E1", "E14", "N1", "W1"]

TARIFF_TYPES = ["Fixed", "Flexible", "Deemed/Out-of-Contract"]


def weighted_choice(pairs):
    options, weights = zip(*pairs)
    return random.choices(options, weights=weights, k=1)[0]


def gen_mpan():
    # Real UK MPAN core is 13 digits. We prefix a fake 2-digit profile class + 2-digit meter
    # timeswitch code (common real-world convention) then 8 unique digits + simple check digit.
    profile_class = str(random.choice([1, 2, 3, 4, 8])).zfill(2)
    meter_timeswitch = str(random.randint(1, 99)).zfill(3)
    unique = str(random.randint(10000000, 99999999))
    return f"{profile_class}{meter_timeswitch}{unique}"


def gen_postcode(region):
    if region == "London":
        prefix = random.choice(LONDON_POSTCODE_PREFIXES)
        return f"{prefix} {random.randint(1,9)}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"
    return fake.postcode()


def generate_customers(n):
    rows = []
    for i in range(1, n + 1):
        customer_id = f"CUST{i:05d}"
        rows.append({
            "customer_id": customer_id,
            "company_name": fake.company(),
            "sector": random.choice(SECTORS),
            "companies_house_no": f"{random.randint(1000000, 9999999):08d}",
            "account_manager": fake.name(),
            "customer_since": fake.date_between(date(2018, 1, 1), date(2024, 1, 1)).isoformat(),
            "credit_rating": random.choice(["A", "A", "B", "B", "B", "C"]),  # weighted toward A/B
        })
    return rows


def generate_sites(customers):
    rows = []
    site_counter = 1
    for cust in customers:
        # Most customers have 1 site, some have 2-4 (multi-site retail/logistics chains)
        num_sites = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
        for _ in range(num_sites):
            region = weighted_choice(REGIONS)
            site_id = f"SITE{site_counter:05d}"
            rows.append({
                "site_id": site_id,
                "customer_id": cust["customer_id"],
                "site_name": f"{cust['company_name']} - {fake.city()} site",
                "mpan": gen_mpan(),
                "postcode": gen_postcode(region),
                "region": region,
                "meter_type": random.choices(
                    ["HH Smart Meter", "AMR (Automated Meter Reading)"], weights=[80, 20]
                )[0],
                "annual_baseline_kwh": random.choice([
                    random.randint(50_000, 200_000),     # small office/retail
                    random.randint(200_000, 800_000),     # mid manufacturing/logistics
                    random.randint(800_000, 3_000_000),   # large site / data centre
                ]),
            })
            site_counter += 1
    return rows


def generate_contracts(sites):
    """
    Generates a contract HISTORY per site (SCD2 style: multiple rows per site_id
    over time, each with its own validity window). Roughly 1-2 renewals across
    2024-01-01 to 2026-06-25 per site.
    """
    rows = []
    contract_counter = 1
    project_end = date(2026, 6, 25)

    for site in sites:
        cursor = date(2024, 1, 1)
        # Slight variation so not every contract starts exactly on the same day
        cursor -= timedelta(days=random.randint(0, 60))

        while cursor < project_end:
            contract_id = f"CON{contract_counter:05d}"
            tariff_type = random.choices(TARIFF_TYPES, weights=[55, 35, 10])[0]
            duration_months = random.choice([12, 12, 24, 36])
            end_date = min(
                date(cursor.year + duration_months // 12, ((cursor.month - 1 + duration_months) % 12) + 1, 1)
                - timedelta(days=1),
                project_end + timedelta(days=365),  # allow contracts to extend past project end
            )

            base_rate = round(random.uniform(0.18, 0.32), 4)  # GBP per kWh, realistic 2024-2026 C&I range
            contracted_annual_kwh = site["annual_baseline_kwh"] * random.uniform(0.9, 1.1)

            rows.append({
                "contract_id": contract_id,
                "site_id": site["site_id"],
                "tariff_type": tariff_type,
                "contracted_annual_kwh": round(contracted_annual_kwh, 1),
                "rate_gbp_per_kwh": base_rate,
                "standing_charge_gbp_per_day": round(random.uniform(0.45, 1.85), 4),
                "start_date": cursor.isoformat(),
                "end_date": end_date.isoformat(),
                "is_current": end_date >= project_end,
            })
            contract_counter += 1
            cursor = end_date + timedelta(days=1)

    return rows


def write_csv(rows, path, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows):,} rows -> {path}")


if __name__ == "__main__":
    customers = generate_customers(NUM_CUSTOMERS)
    sites = generate_sites(customers)
    contracts = generate_contracts(sites)

    write_csv(customers, "/home/claude/dataset_gen/customers.csv",
              ["customer_id", "company_name", "sector", "companies_house_no",
               "account_manager", "customer_since", "credit_rating"])

    write_csv(sites, "/home/claude/dataset_gen/sites.csv",
              ["site_id", "customer_id", "site_name", "mpan", "postcode",
               "region", "meter_type", "annual_baseline_kwh"])

    write_csv(contracts, "/home/claude/dataset_gen/contracts.csv",
              ["contract_id", "site_id", "tariff_type", "contracted_annual_kwh",
               "rate_gbp_per_kwh", "standing_charge_gbp_per_day", "start_date",
               "end_date", "is_current"])

    print(f"\nSummary: {len(customers)} customers, {len(sites)} sites, {len(contracts)} contracts")
