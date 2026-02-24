"""
Food Delivery Event Generator
==============================
Generates two synthetic streaming feeds:
  1. Order Events   – full order lifecycle (placed → assigned → picked up → delivered/cancelled)
  2. Courier Events – courier status updates (online → available → picking_up → delivering → offline)

Supports:
  - Realistic time distributions (lunch/dinner peaks, weekday/weekend differences, zone skew)
  - Configurable parameters (restaurants, couriers, zones, surge, cancellation rate, promo rate)
  - Edge cases: out-of-order events, duplicates, missing steps, impossible durations,
    courier going offline mid-delivery

Usage:
    python generator.py [OPTIONS]

Options:
    --num-orders        Total orders to generate (default: 200)
    --num-couriers      Number of couriers (default: 20)
    --num-restaurants   Number of restaurants (default: 30)
    --num-zones         Number of geographic zones (default: 5)
    --cancel-prob       Cancellation probability 0-1 (default: 0.1)
    --promo-prob        Probability a promo is applied 0-1 (default: 0.2)
    --duplicate-prob    Probability of injecting a duplicate event (default: 0.05)
    --late-prob         Probability of injecting a late/out-of-order event (default: 0.08)
    --surge-factor      Demand multiplier during peak hours (default: 2.5)
    --output-dir        Directory to write output files (default: ./sample_data)
    --seed              Random seed for reproducibility (default: 42)

Output files (in --output-dir):
    order_events.json
    order_events.avro
    courier_events.json
    courier_events.avro
"""

import argparse
import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import List, Dict, Any, Optional

import fastavro

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULTS = {
    "num_orders": 200,
    "num_couriers": 20,
    "num_restaurants": 30,
    "num_zones": 5,
    "cancel_prob": 0.10,
    "promo_prob": 0.20,
    "duplicate_prob": 0.05,
    "late_prob": 0.08,
    "surge_factor": 2.5,
    "output_dir": "./sample_data",
    "seed": 42,
}

CANCELLATION_REASONS = [
    "customer_cancelled",
    "restaurant_closed",
    "no_courier_available",
    "payment_failed",
    "item_unavailable",
]

# Zone definitions: name → (lat_center, lon_center, radius_deg, weight)
# Weight controls how much demand this zone gets (skew)
ZONE_TEMPLATES = [
    ("zone_north",     40.440,  -3.690, 0.03, 0.25),
    ("zone_south",     40.390,  -3.700, 0.03, 0.20),
    ("zone_center",    40.416,  -3.703, 0.02, 0.30),
    ("zone_east",      40.420,  -3.660, 0.03, 0.15),
    ("zone_west",      40.415,  -3.740, 0.03, 0.10),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_uuid() -> str:
    return str(uuid.uuid4())


def epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def random_coords(zone: Dict) -> (float, float):
    """Random lat/lon near zone center."""
    lat = zone["lat"] + random.uniform(-zone["radius"], zone["radius"])
    lon = zone["lon"] + random.uniform(-zone["radius"], zone["radius"])
    return round(lat, 6), round(lon, 6)


def demand_multiplier(hour: int, is_weekend: bool) -> float:
    """
    Returns a demand multiplier for a given hour of day.
    Peaks at lunch (12-14) and dinner (19-22).
    Weekends have a flatter but higher overall curve.
    """
    # Base hourly weights (index = hour 0-23)
    weekday_weights = [
        0.1, 0.05, 0.05, 0.05, 0.05, 0.1,   # 0-5  (night/early morning)
        0.2, 0.4,  0.5,  0.5,  0.6,  0.8,   # 6-11 (morning ramp)
        1.5, 1.8,  1.2,  0.8,  0.7,  0.9,   # 12-17 (lunch peak + afternoon)
        1.3, 2.0,  2.0,  1.5,  0.8,  0.3,   # 18-23 (dinner peak)
    ]
    weekend_weights = [
        0.2, 0.1,  0.1,  0.1,  0.1,  0.1,
        0.2, 0.3,  0.5,  0.8,  1.0,  1.2,
        1.6, 1.8,  1.5,  1.2,  1.0,  1.1,
        1.4, 1.9,  1.9,  1.6,  1.0,  0.5,
    ]
    weights = weekend_weights if is_weekend else weekday_weights
    return weights[hour]


def sample_order_time(rng: random.Random, base_date: datetime, surge_factor: float) -> datetime:
    """
    Sample a realistic order timestamp using rejection sampling
    against the hourly demand curve.
    """
    is_weekend = base_date.weekday() >= 5
    max_weight = surge_factor * 2.0  # upper bound for rejection sampling
    while True:
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        w = demand_multiplier(hour, is_weekend) * (surge_factor if hour in range(12, 15) or hour in range(19, 23) else 1.0)
        if rng.uniform(0, max_weight) < w:
            return base_date.replace(hour=hour, minute=minute, second=second, microsecond=0)


# ---------------------------------------------------------------------------
# Entity generation
# ---------------------------------------------------------------------------

def build_zones(n: int) -> List[Dict]:
    templates = ZONE_TEMPLATES[:n]
    zones = []
    for name, lat, lon, radius, weight in templates:
        zones.append({"id": name, "lat": lat, "lon": lon, "radius": radius, "weight": weight})
    # Normalise weights
    total = sum(z["weight"] for z in zones)
    for z in zones:
        z["weight"] /= total
    return zones


def build_restaurants(n: int, zones: List[Dict], rng: random.Random) -> List[Dict]:
    restaurants = []
    zone_ids = [z["id"] for z in zones]
    zone_weights = [z["weight"] for z in zones]
    for i in range(n):
        zone_id = rng.choices(zone_ids, weights=zone_weights)[0]
        restaurants.append({"id": f"rest_{i+1:03d}", "zone_id": zone_id})
    return restaurants


def build_couriers(n: int, zones: List[Dict], rng: random.Random) -> List[Dict]:
    couriers = []
    zone_ids = [z["id"] for z in zones]
    zone_weights = [z["weight"] for z in zones]
    for i in range(n):
        zone_id = rng.choices(zone_ids, weights=zone_weights)[0]
        couriers.append({"id": f"courier_{i+1:03d}", "zone_id": zone_id})
    return couriers


def build_customers(n: int, rng: random.Random) -> List[str]:
    return [f"customer_{i+1:04d}" for i in range(n)]


# ---------------------------------------------------------------------------
# Order event generation
# ---------------------------------------------------------------------------

def generate_order_events(
    cfg: Dict,
    zones: List[Dict],
    restaurants: List[Dict],
    couriers: List[Dict],
    customers: List[str],
    rng: random.Random,
    base_date: datetime,
) -> List[Dict]:
    """Generate a full set of order lifecycle events."""
    events = []
    zone_map = {z["id"]: z for z in zones}
    zone_ids = [z["id"] for z in zones]
    zone_weights = [z["weight"] for z in zones]

    for _ in range(cfg["num_orders"]):
        order_id = new_uuid()
        customer_id = rng.choice(customers)
        restaurant = rng.choice(restaurants)
        zone_id = restaurant["zone_id"]
        zone = zone_map[zone_id]
        courier = rng.choice(couriers)
        order_value = round(rng.uniform(8.0, 65.0), 2)
        promo = rng.random() < cfg["promo_prob"]
        if promo:
            order_value = round(order_value * rng.uniform(0.7, 0.9), 2)

        # Sample base order time
        placed_dt = sample_order_time(rng, base_date, cfg["surge_factor"])

        # Decide if this order gets cancelled
        cancelled = rng.random() < cfg["cancel_prob"]

        # Decide if this order is missing a step (e.g. delivered without picked_up)
        missing_pickup = (not cancelled) and (rng.random() < 0.03)

        # Decide if this order has an "impossible" duration (anomaly)
        impossible_duration = (not cancelled) and (rng.random() < 0.02)

        # Build event chain
        order_events = []

        # --- ORDER_PLACED ---
        order_events.append({
            "event_id": new_uuid(),
            "order_id": order_id,
            "event_type": "ORDER_PLACED",
            "timestamp": epoch_ms(placed_dt),
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "courier_id": None,
            "zone_id": zone_id,
            "order_value": order_value,
            "promo_applied": promo,
            "cancellation_reason": None,
            "is_duplicate": False,
            "schema_version": "1.0",
        })

        if cancelled:
            # Cancel happens shortly after placement
            cancel_dt = placed_dt + timedelta(seconds=rng.randint(30, 300))
            order_events.append({
                "event_id": new_uuid(),
                "order_id": order_id,
                "event_type": "CANCELLED",
                "timestamp": epoch_ms(cancel_dt),
                "customer_id": customer_id,
                "restaurant_id": restaurant["id"],
                "courier_id": None,
                "zone_id": zone_id,
                "order_value": order_value,
                "promo_applied": promo,
                "cancellation_reason": rng.choice(CANCELLATION_REASONS),
                "is_duplicate": False,
                "schema_version": "1.0",
            })
        else:
            # --- COURIER_ASSIGNED ---
            assign_dt = placed_dt + timedelta(seconds=rng.randint(30, 120))
            order_events.append({
                "event_id": new_uuid(),
                "order_id": order_id,
                "event_type": "COURIER_ASSIGNED",
                "timestamp": epoch_ms(assign_dt),
                "customer_id": customer_id,
                "restaurant_id": restaurant["id"],
                "courier_id": courier["id"],
                "zone_id": zone_id,
                "order_value": order_value,
                "promo_applied": promo,
                "cancellation_reason": None,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

            if not missing_pickup:
                # --- PICKED_UP ---
                prep_secs = rng.randint(300, 1500)  # 5–25 min prep
                pickup_dt = assign_dt + timedelta(seconds=prep_secs)
                order_events.append({
                    "event_id": new_uuid(),
                    "order_id": order_id,
                    "event_type": "PICKED_UP",
                    "timestamp": epoch_ms(pickup_dt),
                    "customer_id": customer_id,
                    "restaurant_id": restaurant["id"],
                    "courier_id": courier["id"],
                    "zone_id": zone_id,
                    "order_value": order_value,
                    "promo_applied": promo,
                    "cancellation_reason": None,
                    "is_duplicate": False,
                    "schema_version": "1.0",
                })
            else:
                pickup_dt = assign_dt + timedelta(seconds=rng.randint(300, 900))

            # --- DELIVERED ---
            if impossible_duration:
                # "Impossible" delivery: only 5 seconds after pickup
                delivery_secs = rng.randint(1, 10)
            else:
                delivery_secs = rng.randint(600, 2400)  # 10–40 min delivery
            delivered_dt = pickup_dt + timedelta(seconds=delivery_secs)
            order_events.append({
                "event_id": new_uuid(),
                "order_id": order_id,
                "event_type": "DELIVERED",
                "timestamp": epoch_ms(delivered_dt),
                "customer_id": customer_id,
                "restaurant_id": restaurant["id"],
                "courier_id": courier["id"],
                "zone_id": zone_id,
                "order_value": order_value,
                "promo_applied": promo,
                "cancellation_reason": None,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

        events.extend(order_events)

        # --- INJECT DUPLICATE ---
        if order_events and rng.random() < cfg["duplicate_prob"]:
            dup = dict(order_events[0])
            dup["event_id"] = new_uuid()
            dup["is_duplicate"] = True
            events.append(dup)

    # --- INJECT LATE / OUT-OF-ORDER EVENTS ---
    # Take a random subset of events and shift their timestamp back
    num_late = int(len(events) * cfg["late_prob"])
    late_candidates = [e for e in events if e["event_type"] not in ("ORDER_PLACED",)]
    rng.shuffle(late_candidates)
    for e in late_candidates[:num_late]:
        # Shift timestamp forward in processing time but backward in event time
        e["timestamp"] -= rng.randint(300_000, 900_000)  # 5–15 min late

    # Shuffle events to simulate out-of-order arrival
    rng.shuffle(events)

    return events


# ---------------------------------------------------------------------------
# Courier event generation
# ---------------------------------------------------------------------------

def generate_courier_events(
    cfg: Dict,
    zones: List[Dict],
    couriers: List[Dict],
    order_events: List[Dict],
    rng: random.Random,
    base_date: datetime,
) -> List[Dict]:
    """Generate courier status events, anchored to order events where possible."""
    events = []
    zone_map = {z["id"]: z for z in zones}

    # Build a lookup: courier_id → list of (order_id, assigned_ts, pickup_ts, delivered_ts)
    courier_orders: Dict[str, List] = {c["id"]: [] for c in couriers}
    order_map: Dict[str, Dict] = {}
    for e in order_events:
        oid = e["order_id"]
        if oid not in order_map:
            order_map[oid] = {}
        order_map[oid][e["event_type"]] = e

    for oid, lifecycle in order_map.items():
        if "COURIER_ASSIGNED" in lifecycle and "DELIVERED" in lifecycle:
            cid = lifecycle["COURIER_ASSIGNED"]["courier_id"]
            if cid:
                courier_orders[cid].append({
                    "order_id": oid,
                    "zone_id": lifecycle["COURIER_ASSIGNED"]["zone_id"],
                    "assigned_ts": lifecycle["COURIER_ASSIGNED"]["timestamp"],
                    "pickup_ts": lifecycle.get("PICKED_UP", lifecycle["COURIER_ASSIGNED"])["timestamp"],
                    "delivered_ts": lifecycle["DELIVERED"]["timestamp"],
                })

    for courier in couriers:
        cid = courier["id"]
        zone = zone_map.get(courier["zone_id"], zone_map[list(zone_map.keys())[0]])
        session_id = new_uuid()

        # Courier comes online at a random time during the day
        online_hour = rng.choice([10, 11, 17, 18, 19])
        online_dt = base_date.replace(
            hour=online_hour,
            minute=rng.randint(0, 59),
            second=rng.randint(0, 59),
        )
        lat, lon = random_coords(zone)

        # ONLINE
        events.append({
            "event_id": new_uuid(),
            "courier_id": cid,
            "event_type": "ONLINE",
            "timestamp": epoch_ms(online_dt),
            "zone_id": zone["id"],
            "latitude": lat,
            "longitude": lon,
            "order_id": None,
            "session_id": session_id,
            "is_duplicate": False,
            "schema_version": "1.0",
        })

        # AVAILABLE right after coming online
        avail_dt = online_dt + timedelta(seconds=rng.randint(10, 60))
        lat, lon = random_coords(zone)
        events.append({
            "event_id": new_uuid(),
            "courier_id": cid,
            "event_type": "AVAILABLE",
            "timestamp": epoch_ms(avail_dt),
            "zone_id": zone["id"],
            "latitude": lat,
            "longitude": lon,
            "order_id": None,
            "session_id": session_id,
            "is_duplicate": False,
            "schema_version": "1.0",
        })

        # Process deliveries assigned to this courier
        deliveries = sorted(courier_orders[cid], key=lambda x: x["assigned_ts"])
        went_offline_mid = False

        for i, delivery in enumerate(deliveries):
            # Decide if courier goes offline mid-delivery (edge case)
            if not went_offline_mid and i == len(deliveries) // 2 and rng.random() < 0.05:
                # Courier drops offline unexpectedly
                offline_dt = datetime.fromtimestamp(delivery["assigned_ts"] / 1000, tz=timezone.utc).replace(tzinfo=None)
                offline_dt += timedelta(seconds=rng.randint(60, 300))
                lat, lon = random_coords(zone)
                events.append({
                    "event_id": new_uuid(),
                    "courier_id": cid,
                    "event_type": "OFFLINE",
                    "timestamp": epoch_ms(offline_dt),
                    "zone_id": zone["id"],
                    "latitude": lat,
                    "longitude": lon,
                    "order_id": delivery["order_id"],  # offline while on a delivery!
                    "session_id": session_id,
                    "is_duplicate": False,
                    "schema_version": "1.0",
                })
                went_offline_mid = True
                break

            dzone = zone_map.get(delivery["zone_id"], zone)

            # PICKING_UP
            lat, lon = random_coords(dzone)
            events.append({
                "event_id": new_uuid(),
                "courier_id": cid,
                "event_type": "PICKING_UP",
                "timestamp": delivery["pickup_ts"],
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": delivery["order_id"],
                "session_id": session_id,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

            # DELIVERING
            lat, lon = random_coords(dzone)
            mid_ts = (delivery["pickup_ts"] + delivery["delivered_ts"]) // 2
            events.append({
                "event_id": new_uuid(),
                "courier_id": cid,
                "event_type": "DELIVERING",
                "timestamp": mid_ts,
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": delivery["order_id"],
                "session_id": session_id,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

            # AVAILABLE again after delivery
            after_dt = datetime.fromtimestamp(delivery["delivered_ts"] / 1000, tz=timezone.utc).replace(tzinfo=None)
            after_dt += timedelta(seconds=rng.randint(30, 120))
            lat, lon = random_coords(dzone)
            events.append({
                "event_id": new_uuid(),
                "courier_id": cid,
                "event_type": "AVAILABLE",
                "timestamp": epoch_ms(after_dt),
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": None,
                "session_id": session_id,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

        if not went_offline_mid:
            # OFFLINE at end of shift
            last_ts = max((e["timestamp"] for e in events if e.get("courier_id") == cid), default=epoch_ms(online_dt))
            offline_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).replace(tzinfo=None)
            offline_dt += timedelta(minutes=rng.randint(5, 30))
            lat, lon = random_coords(zone)
            events.append({
                "event_id": new_uuid(),
                "courier_id": cid,
                "event_type": "OFFLINE",
                "timestamp": epoch_ms(offline_dt),
                "zone_id": zone["id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": None,
                "session_id": session_id,
                "is_duplicate": False,
                "schema_version": "1.0",
            })

        # --- INJECT DUPLICATE ---
        if rng.random() < cfg["duplicate_prob"]:
            dup_source = rng.choice([e for e in events if e.get("courier_id") == cid])
            dup = dict(dup_source)
            dup["event_id"] = new_uuid()
            dup["is_duplicate"] = True
            events.append(dup)

    # --- INJECT LATE COURIER EVENTS ---
    num_late = int(len(events) * cfg["late_prob"])
    rng.shuffle(events)
    for e in events[:num_late]:
        e["timestamp"] -= rng.randint(180_000, 600_000)

    rng.shuffle(events)
    return events


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def load_schema(path: str) -> Dict:
    with open(path) as f:
        return json.load(f)


def write_json(events: List[Dict], path: str):
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"  Written {len(events)} events → {path}")


def write_avro(events: List[Dict], schema_dict: Dict, path: str):
    parsed_schema = fastavro.parse_schema(schema_dict)
    with open(path, "wb") as f:
        fastavro.writer(f, parsed_schema, events)
    print(f"  Written {len(events)} events → {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Food Delivery Streaming Event Generator")
    p.add_argument("--num-orders",      type=int,   default=DEFAULTS["num_orders"])
    p.add_argument("--num-couriers",    type=int,   default=DEFAULTS["num_couriers"])
    p.add_argument("--num-restaurants", type=int,   default=DEFAULTS["num_restaurants"])
    p.add_argument("--num-zones",       type=int,   default=DEFAULTS["num_zones"],
                   choices=range(1, 6), metavar="1-5")
    p.add_argument("--cancel-prob",     type=float, default=DEFAULTS["cancel_prob"])
    p.add_argument("--promo-prob",      type=float, default=DEFAULTS["promo_prob"])
    p.add_argument("--duplicate-prob",  type=float, default=DEFAULTS["duplicate_prob"])
    p.add_argument("--late-prob",       type=float, default=DEFAULTS["late_prob"])
    p.add_argument("--surge-factor",    type=float, default=DEFAULTS["surge_factor"])
    p.add_argument("--output-dir",      type=str,   default=DEFAULTS["output_dir"])
    p.add_argument("--seed",            type=int,   default=DEFAULTS["seed"])
    return p.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    cfg = {
        "num_orders":      args.num_orders,
        "num_couriers":    args.num_couriers,
        "num_restaurants": args.num_restaurants,
        "num_zones":       args.num_zones,
        "cancel_prob":     args.cancel_prob,
        "promo_prob":      args.promo_prob,
        "duplicate_prob":  args.duplicate_prob,
        "late_prob":       args.late_prob,
        "surge_factor":    args.surge_factor,
    }

    os.makedirs(args.output_dir, exist_ok=True)

    # Use "today" as the simulation date
    base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"\n🍕  Food Delivery Event Generator")
    print(f"   Date        : {base_date.date()}")
    print(f"   Orders      : {cfg['num_orders']}")
    print(f"   Couriers    : {cfg['num_couriers']}")
    print(f"   Restaurants : {cfg['num_restaurants']}")
    print(f"   Zones       : {cfg['num_zones']}")
    print(f"   Seed        : {args.seed}\n")

    # Build entities
    zones       = build_zones(cfg["num_zones"])
    restaurants = build_restaurants(cfg["num_restaurants"], zones, rng)
    couriers    = build_couriers(cfg["num_couriers"], zones, rng)
    customers   = build_customers(max(50, cfg["num_orders"] // 3), rng)

    # Generate events
    print("Generating order events...")
    order_events = generate_order_events(cfg, zones, restaurants, couriers, customers, rng, base_date)

    print("Generating courier events...")
    courier_events = generate_courier_events(cfg, zones, couriers, order_events, rng, base_date)

    # Load schemas
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    order_schema  = load_schema(os.path.join(script_dir, "schemas", "order_event.avsc"))
    courier_schema = load_schema(os.path.join(script_dir, "schemas", "courier_event.avsc"))

    # Write outputs
    print("\nWriting output files...")
    write_json(order_events,   os.path.join(args.output_dir, "order_events.json"))
    write_avro(order_events,   order_schema,  os.path.join(args.output_dir, "order_events.avro"))
    write_json(courier_events, os.path.join(args.output_dir, "courier_events.json"))
    write_avro(courier_events, courier_schema, os.path.join(args.output_dir, "courier_events.avro"))

    # Print summary stats
    order_types = {}
    for e in order_events:
        order_types[e["event_type"]] = order_types.get(e["event_type"], 0) + 1
    courier_types = {}
    for e in courier_events:
        courier_types[e["event_type"]] = courier_types.get(e["event_type"], 0) + 1

    duplicates_o = sum(1 for e in order_events if e["is_duplicate"])
    duplicates_c = sum(1 for e in courier_events if e["is_duplicate"])

    print(f"\n✅  Done!")
    print(f"\nOrder event breakdown:   {order_types}")
    print(f"Courier event breakdown: {courier_types}")
    print(f"Duplicate order events:  {duplicates_o}")
    print(f"Duplicate courier events:{duplicates_c}")
    print(f"\nAll files written to: {args.output_dir}/\n")


if __name__ == "__main__":
    main()
