"""Append fake e-commerce users to `staging.core_user` in the Lakekeeper catalog.

Append-only (no updates or deletes), so every run is one append snapshot:
  - the first run creates the namespace and table with NUM_ROWS new users
  - later runs append NUM_ROWS new users (user_ids continue from the current
    max) plus a new version of UPDATE_FRACTION of existing users: same user_id,
    changed login/activity/tier/opt-in (sometimes phone/address) and a newer
    `updated_at`. The latest `updated_at` per user_id is the current state.

Run from the `etl` dir with the stack up (`just start`):

    uv run python src/data_gen/gen_staging_table.py
"""

import os
import random
from datetime import UTC, datetime, timedelta

import polars as pl
from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table

NAMESPACE = "staging"
TABLE_NAME = "core_user"
NUM_ROWS = 10_000
UPDATE_FRACTION = 0.4
SEED = 42

WAREHOUSE_ROOT = "s3://warehouse"
NAMESPACE_LOCATION = f"{WAREHOUSE_ROOT}/{NAMESPACE}"
TABLE_LOCATION = f"{NAMESPACE_LOCATION}/{TABLE_NAME}"

CATALOG_URI = os.getenv("CATALOG_URI", "http://localhost:8181/catalog")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

S3_PROPERTIES = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": S3_ACCESS_KEY,
    "s3.secret-access-key": S3_SECRET_KEY,
    "s3.region": S3_REGION,
    "s3.path-style-access": "true",
}

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
LOYALTY_WEIGHTS = [0.6, 0.25, 0.12, 0.03]
GENDERS = ["female", "male", "non_binary", "undisclosed"]
GENDER_WEIGHTS = [0.48, 0.47, 0.02, 0.03]


def generate_users(start_id: int, num_rows: int) -> pl.DataFrame:
    # Seed per batch so each run is reproducible but differs from the last.
    fake = Faker()
    Faker.seed(SEED + start_id)
    rng = random.Random(SEED + start_id)
    now = datetime.now(UTC).replace(microsecond=0)

    rows = []
    for user_id in range(start_id, start_id + num_rows):
        first_name = fake.first_name()
        last_name = fake.last_name()
        signup_ts = fake.date_time_between(start_date="-5y", end_date="-1d", tzinfo=UTC)
        last_login_ts = (
            fake.date_time_between(start_date=signup_ts, end_date=now, tzinfo=UTC)
            if rng.random() < 0.9
            else None
        )
        rows.append(
            {
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                # user_id suffix keeps emails unique across batches
                "email": f"{first_name}.{last_name}{user_id}@{fake.free_email_domain()}".lower(),
                "phone": fake.phone_number(),
                "gender": rng.choices(GENDERS, GENDER_WEIGHTS)[0],
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
                "street_address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postal_code": fake.postcode(),
                "country": "US",
                "loyalty_tier": rng.choices(LOYALTY_TIERS, LOYALTY_WEIGHTS)[0],
                "marketing_opt_in": rng.random() < 0.55,
                "is_active": last_login_ts is not None
                and last_login_ts > now - timedelta(days=365),
                "signup_ts": signup_ts,
                "last_login_ts": last_login_ts,
                "updated_at": now,
            }
        )

    return pl.DataFrame(
        rows,
        schema_overrides={
            "user_id": pl.Int64,
            "date_of_birth": pl.Date,
            "signup_ts": pl.Datetime("us", "UTC"),
            "last_login_ts": pl.Datetime("us", "UTC"),
            "updated_at": pl.Datetime("us", "UTC"),
        },
    )


def latest_users(table: Table) -> pl.DataFrame:
    """Current state of each user: the row with the newest updated_at."""
    return (
        table.scan()
        .to_polars()
        .sort("updated_at")
        .unique(subset="user_id", keep="last")
        .sort("user_id")
    )


def generate_user_updates(users: pl.DataFrame, fraction: float, seed: int) -> pl.DataFrame:
    """New versions of a random `fraction` of users, with realistic changes."""
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)
    now = datetime.now(UTC).replace(microsecond=0)

    rows = []
    for row in users.sample(fraction=fraction, seed=seed).iter_rows(named=True):
        last_seen = row["last_login_ts"] or row["signup_ts"]
        row["last_login_ts"] = fake.date_time_between(start_date=last_seen, end_date=now, tzinfo=UTC)
        row["is_active"] = True
        if rng.random() < 0.15:
            row["loyalty_tier"] = rng.choices(LOYALTY_TIERS, LOYALTY_WEIGHTS)[0]
        if rng.random() < 0.2:
            row["marketing_opt_in"] = not row["marketing_opt_in"]
        if rng.random() < 0.05:
            row["phone"] = fake.phone_number()
        if rng.random() < 0.05:
            row["street_address"] = fake.street_address()
            row["city"] = fake.city()
            row["state"] = fake.state_abbr()
            row["postal_code"] = fake.postcode()
        row["updated_at"] = now
        rows.append(row)

    return pl.DataFrame(rows, schema=users.schema)


def with_client_s3(table: Table) -> Table:
    # The catalog hands out its own S3 endpoint (http://silo:9000), which only
    # resolves inside the docker network. Point the FileIO back at ours and
    # pin PyArrow so a server-suggested fsspec FileIO (needs s3fs) isn't used.
    table.io = load_file_io(
        {
            **table.io.properties,
            **S3_PROPERTIES,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
        table.metadata_location,
    )
    return table


def main() -> None:
    catalog = load_catalog(
        "iceberg",
        type="rest",
        uri=CATALOG_URI,
        warehouse="warehouse",
        **S3_PROPERTIES,
    )

    # Without an explicit location Lakekeeper puts tables at <warehouse>/<uuid>;
    # keep them under s3://warehouse/<namespace>/ instead.
    catalog.create_namespace_if_not_exists(NAMESPACE, {"location": NAMESPACE_LOCATION})
    identifier = (NAMESPACE, TABLE_NAME)

    num_updates = 0
    if catalog.table_exists(identifier):
        table = with_client_s3(catalog.load_table(identifier))
        users = latest_users(table)
        start_id = (users.select(pl.col("user_id").max()).item() or 0) + 1
        updates = generate_user_updates(users, UPDATE_FRACTION, SEED + start_id)
        num_updates = updates.height
        data = pl.concat([updates, generate_users(start_id, NUM_ROWS)]).to_arrow()
    else:
        start_id = 1
        data = generate_users(start_id, NUM_ROWS).to_arrow()
        table = with_client_s3(
            catalog.create_table(identifier, schema=data.schema, location=TABLE_LOCATION)
        )

    table.append(data)

    table = catalog.load_table(identifier)
    print(
        f"Appended {NUM_ROWS} new users (user_ids {start_id}..{start_id + NUM_ROWS - 1}) "
        f"and {num_updates} updated user versions to {NAMESPACE}.{TABLE_NAME}"
    )
    print(f"Location: {table.location()}")
    snapshot = table.current_snapshot()
    rows = snapshot.summary["total-records"] if snapshot and snapshot.summary else 0
    print(f"Total rows: {rows}")
    print(f"Snapshots: {len(table.snapshots())}")


if __name__ == "__main__":
    main()
