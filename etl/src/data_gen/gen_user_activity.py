"""Append fake mobile-app activity events to `staging.user_activity` (Lakekeeper).

Events are modelled on a Mixpanel export: one row per event, `t` is the event
name (always one of EVENT_TYPES), and Mixpanel's reserved properties
($insert_id, distinct_id, $device_id, mp_lib, $os, $model, ...) are flattened
into columns without the `$` prefix. Event-specific properties (product, cart,
order, search, ...) are nullable columns, filled only for events that carry them.

Every run appends NUM_SESSIONS app sessions for random users from
`staging.core_user` (run `just seed` first). A session walks a shopping funnel
with drop-offs:

    [Push Notification Opened] -> App Open -> [Sign In]
      -> (Screen View, [Product Search], Product Viewed, [Add To Cart]) x 1..4
      -> [Remove From Cart] -> [Checkout Started] -> [Order Completed]
      -> $ae_session            ($ae_crashed ends a session early)

Append-only: no updates or deletes, so every run is one append snapshot.

Run from the `etl` dir with the stack up (`just start`):

    uv run python src/data_gen/gen_user_activity.py
"""

import os
import random
import uuid
from datetime import UTC, datetime, timedelta

import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

NAMESPACE = "staging"
TABLE_NAME = "user_activity"
USERS_TABLE = "core_user"
NUM_SESSIONS = 2_000
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

# The complete, fixed set of values `t` can take.
EVENT_TYPES = (
    "$ae_session",  # Mixpanel automatic mobile event: session ended
    "$ae_crashed",  # Mixpanel automatic mobile event: app crashed
    "Push Notification Opened",
    "App Open",
    "Sign In",
    "Screen View",
    "Product Search",
    "Product Viewed",
    "Add To Cart",
    "Remove From Cart",
    "Checkout Started",
    "Order Completed",
)

CAMPAIGNS = ("flash_sale", "cart_reminder", "back_in_stock", "weekly_digest")

# (product_id, name, category, price)
PRODUCTS = (
    ("SKU-1001", "Wireless Earbuds", "Electronics", 79.99),
    ("SKU-1002", "Smart Watch", "Electronics", 199.00),
    ("SKU-1003", "Phone Case", "Electronics", 19.99),
    ("SKU-1004", "USB-C Charger", "Electronics", 29.99),
    ("SKU-2001", "Running Shoes", "Footwear", 119.00),
    ("SKU-2002", "Trail Sneakers", "Footwear", 134.50),
    ("SKU-2003", "Slides", "Footwear", 24.00),
    ("SKU-3001", "Denim Jacket", "Apparel", 89.00),
    ("SKU-3002", "Cotton T-Shirt", "Apparel", 18.00),
    ("SKU-3003", "Hoodie", "Apparel", 54.00),
    ("SKU-4001", "Coffee Grinder", "Home & Kitchen", 64.99),
    ("SKU-4002", "French Press", "Home & Kitchen", 32.00),
    ("SKU-4003", "Chef Knife", "Home & Kitchen", 79.00),
    ("SKU-5001", "Yoga Mat", "Sports", 39.99),
    ("SKU-5002", "Water Bottle", "Sports", 22.50),
    ("SKU-6001", "Face Moisturizer", "Beauty", 27.00),
)

# (mp_lib, os, manufacturer, [models], [os versions])
PLATFORMS = (
    ("swift", "iOS", "Apple", ["iPhone15,2", "iPhone15,4", "iPhone16,1", "iPhone17,3"], ["17.6", "18.3", "18.5", "26.0"]),
    ("android", "Android", "Samsung", ["SM-S921B", "SM-A546E", "SM-S928U"], ["13", "14", "15"]),
    ("android", "Android", "Google", ["Pixel 8", "Pixel 9", "Pixel 9 Pro"], ["14", "15", "16"]),
)
PLATFORM_WEIGHTS = (0.55, 0.3, 0.15)
CARRIERS = ("Verizon", "AT&T", "T-Mobile", "US Cellular")
APP_VERSIONS = (("5.12.0", 51200), ("5.13.0", 51300), ("5.13.1", 51301), ("5.14.0", 51400))
APP_VERSION_WEIGHTS = (0.1, 0.25, 0.4, 0.25)

SCHEMA = {
    "t": pl.String,
    "insert_id": pl.String,
    "distinct_id": pl.String,
    "user_id": pl.Int64,
    "device_id": pl.String,
    "session_id": pl.String,
    "event_time": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"),
    "mp_lib": pl.String,
    "os": pl.String,
    "os_version": pl.String,
    "manufacturer": pl.String,
    "model": pl.String,
    "app_version": pl.String,
    "app_build_number": pl.Int32,
    "carrier": pl.String,
    "wifi": pl.Boolean,
    "city": pl.String,
    "region": pl.String,
    "country_code": pl.String,
    "screen_name": pl.String,
    "campaign": pl.String,
    "search_query": pl.String,
    "product_id": pl.String,
    "product_name": pl.String,
    "category": pl.String,
    "price": pl.Float64,
    "quantity": pl.Int32,
    "cart_value": pl.Float64,
    "order_id": pl.String,
    "session_length_sec": pl.Int32,
}


def device_for(user_id: int) -> dict:
    """One stable device per user, derived from the user_id alone."""
    rng = random.Random(user_id)
    mp_lib, os_name, manufacturer, models, os_versions = rng.choices(PLATFORMS, PLATFORM_WEIGHTS)[0]
    return {
        "device_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "mp_lib": mp_lib,
        "os": os_name,
        "os_version": rng.choice(os_versions),
        "manufacturer": manufacturer,
        "model": rng.choice(models),
        "carrier": rng.choice(CARRIERS),
    }


def generate_session(rng: random.Random, user: dict, start: datetime, ingested_at: datetime) -> list[dict]:
    device = device_for(user["user_id"])
    app_version, build = rng.choices(APP_VERSIONS, APP_VERSION_WEIGHTS)[0]
    base = {
        "distinct_id": str(user["user_id"]),
        "user_id": user["user_id"],
        "session_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "ingested_at": ingested_at,
        **device,
        "app_version": app_version,
        "app_build_number": build,
        "wifi": rng.random() < 0.6,
        "city": user["city"],
        "region": user["state"],
        "country_code": "US",
    }
    events: list[dict] = []
    clock = start

    def emit(t: str, **props) -> None:
        nonlocal clock
        clock += timedelta(seconds=rng.randint(2, 90))
        events.append({**base, "t": t, "insert_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)), "event_time": clock, **props})

    if rng.random() < 0.15:
        emit("Push Notification Opened", campaign=rng.choice(CAMPAIGNS))
    emit("App Open", screen_name="Home")
    if rng.random() < 0.1:
        emit("Sign In")

    cart: list[tuple] = []
    for _ in range(rng.randint(1, 4)):
        product = rng.choice(PRODUCTS)
        product_id, name, category, price = product
        emit("Screen View", screen_name=rng.choice(("Home", "Category", "Search")))
        if rng.random() < 0.5:
            emit("Product Search", screen_name="Search", search_query=name.split()[-1].lower())
        emit("Product Viewed", screen_name="Product Detail", product_id=product_id, product_name=name, category=category, price=price)
        if rng.random() < 0.3:
            quantity = rng.choices((1, 2, 3), (0.8, 0.15, 0.05))[0]
            cart.append((product, quantity))
            emit("Add To Cart", screen_name="Product Detail", product_id=product_id, product_name=name, category=category, price=price, quantity=quantity, cart_value=cart_total(cart))
        if rng.random() < 0.02:
            emit("$ae_crashed", screen_name="Product Detail")
            return events

    if cart and rng.random() < 0.2:
        (product_id, name, category, price), quantity = cart.pop(rng.randrange(len(cart)))
        emit("Remove From Cart", screen_name="Cart", product_id=product_id, product_name=name, category=category, price=price, quantity=quantity, cart_value=cart_total(cart))

    if cart and rng.random() < 0.5:
        items = sum(q for _, q in cart)
        emit("Checkout Started", screen_name="Checkout", quantity=items, cart_value=cart_total(cart))
        if rng.random() < 0.7:
            order_id = f"ORD-{uuid.UUID(int=rng.getrandbits(128), version=4).hex[:12].upper()}"
            emit("Order Completed", screen_name="Order Confirmation", order_id=order_id, quantity=items, cart_value=cart_total(cart))

    emit("$ae_session", session_length_sec=int((clock - start).total_seconds()))
    return events


def cart_total(cart: list[tuple]) -> float:
    return round(sum(product[3] * quantity for product, quantity in cart), 2)


def generate_activity(users: pl.DataFrame, num_sessions: int, seed: int) -> pl.DataFrame:
    rng = random.Random(seed)
    now = datetime.now(UTC).replace(microsecond=0)
    user_rows = users.rows(named=True)

    rows = []
    for _ in range(num_sessions):
        user = rng.choice(user_rows)
        # Sessions started within the last hour; events land a few seconds apart.
        start = now - timedelta(seconds=rng.randint(15 * 60, 60 * 60))
        rows.extend(generate_session(rng, user, start, ingested_at=now))

    df = pl.DataFrame(rows, schema=SCHEMA)
    unknown = set(df["t"].unique()) - set(EVENT_TYPES)
    assert not unknown, f"unexpected event types: {unknown}"
    return df.sort("event_time")


def latest_user_locations(table: Table) -> pl.DataFrame:
    """user_id, city and state from each user's newest version in core_user."""
    return (
        table.scan(selected_fields=("user_id", "city", "state", "updated_at"))
        .to_polars()
        .sort("updated_at")
        .unique(subset="user_id", keep="last")
        .select("user_id", "city", "state")
        .sort("user_id")
    )


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

    users_identifier = (NAMESPACE, USERS_TABLE)
    if not catalog.table_exists(users_identifier):
        raise SystemExit(f"{NAMESPACE}.{USERS_TABLE} not found; run `just seed` first")
    users = latest_user_locations(with_client_s3(catalog.load_table(users_identifier)))

    # Without an explicit location Lakekeeper puts tables at <warehouse>/<uuid>;
    # keep them under s3://warehouse/<namespace>/ instead.
    catalog.create_namespace_if_not_exists(NAMESPACE, {"location": NAMESPACE_LOCATION})
    identifier = (NAMESPACE, TABLE_NAME)

    if catalog.table_exists(identifier):
        table = with_client_s3(catalog.load_table(identifier))
        snapshot = table.current_snapshot()
        existing = int((snapshot.summary["total-records"] if snapshot and snapshot.summary else None) or 0)
    else:
        table = None
        existing = 0

    # Seed from the table's size so each run is reproducible but differs from the last.
    events = generate_activity(users, NUM_SESSIONS, SEED + existing)
    data = events.to_arrow()

    if table is None:
        table = with_client_s3(
            catalog.create_table(identifier, schema=data.schema, location=TABLE_LOCATION)
        )
        with table.update_spec() as spec:
            spec.add_field("event_time", DayTransform(), "event_day")

    table.append(data)

    counts = events.group_by("t").len().sort("len", descending=True)
    table = catalog.load_table(identifier)
    print(f"Appended {data.num_rows} events from {NUM_SESSIONS} sessions to {NAMESPACE}.{TABLE_NAME}")
    for t, n in counts.iter_rows():
        print(f"  {t:<26}{n:>7}")
    print(f"Location: {table.location()}")
    snapshot = table.current_snapshot()
    rows = snapshot.summary["total-records"] if snapshot and snapshot.summary else 0
    print(f"Total rows: {rows}")
    print(f"Snapshots: {len(table.snapshots())}")


if __name__ == "__main__":
    main()
