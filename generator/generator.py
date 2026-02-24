"""
Food Delivery Event Generator
==============================
Generates two synthetic streaming feeds:
  1. Order Events   – full order lifecycle (placed -> assigned -> picked up -> delivered/cancelled)
  2. Courier Events – courier status updates (online -> available -> picking_up -> delivering -> offline)

Supports:
  - Realistic time distributions with weekday/weekend demand curves
  - Zone-aware courier assignment (same zone preferred, availability tracking)
  - Per-restaurant opening hours and prep time distributions
  - Multiple city presets (Madrid, Barcelona, London)
  - Configurable edge cases: duplicates, late events, missing steps, impossible durations,
    courier offline mid-delivery, fraud clusters, zone surge events
  - Streaming simulation mode with configurable speed factor
  - Validation report generation
  - Docker support

Usage:
    python generator.py [OPTIONS]

Run `python generator.py --help` for full option list.
"""

import argparse
import json
import math
import os
import random
import sys
import time as time_module
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

import fastavro

# ---------------------------------------------------------------------------
# Constants & presets
# ---------------------------------------------------------------------------

DEFAULTS = {
    "num_orders": 200,
    "num_couriers": 20,
    "num_restaurants": 30,
    "cancel_prob": 0.10,
    "promo_prob": 0.20,
    "duplicate_prob": 0.05,
    "late_prob": 0.08,
    "missing_step_prob": 0.03,
    "impossible_duration_prob": 0.02,
    "mid_delivery_offline_prob": 0.05,
    "fraud_cluster_prob": 0.02,
    "surge_factor": 2.5,
    "speed_factor": 60,
    "output_dir": "./sample_data",
    "seed": 42,
    "city": "madrid",
}

CANCELLATION_REASONS = [
    "customer_cancelled",
    "restaurant_closed",
    "no_courier_available",
    "payment_failed",
    "item_unavailable",
]

# City zone presets: list of (name, lat, lon, radius_deg, demand_weight)
CITY_PRESETS = {
    "madrid": [
        ("zone_center",    40.416, -3.703, 0.020, 0.30),
        ("zone_north",     40.440, -3.690, 0.030, 0.25),
        ("zone_south",     40.390, -3.700, 0.030, 0.20),
        ("zone_east",      40.420, -3.660, 0.030, 0.15),
        ("zone_west",      40.415, -3.740, 0.030, 0.10),
    ],
    "barcelona": [
        ("zone_eixample",  41.390,  2.165, 0.020, 0.30),
        ("zone_gothic",    41.382,  2.177, 0.020, 0.25),
        ("zone_gracia",    41.403,  2.156, 0.020, 0.20),
        ("zone_born",      41.385,  2.183, 0.020, 0.15),
        ("zone_sants",     41.375,  2.133, 0.030, 0.10),
    ],
    "london": [
        ("zone_soho",       51.513, -0.137, 0.020, 0.30),
        ("zone_shoreditch", 51.524, -0.079, 0.020, 0.25),
        ("zone_camden",     51.539, -0.143, 0.020, 0.20),
        ("zone_southbank",  51.506, -0.115, 0.020, 0.15),
        ("zone_kensington", 51.502, -0.192, 0.020, 0.10),
    ],
}

# Restaurant schedule profiles:
#   (profile_name, open_ranges, probability, avg_prep_secs, prep_std_secs)
RESTAURANT_PROFILES = [
    ("all_day",      [(10, 23)],           0.50, 900,  300),
    ("lunch_dinner", [(12, 15), (19, 23)], 0.30, 720,  240),
    ("dinner_only",  [(18, 23)],           0.20, 1200, 420),
]

# Hourly demand weights (index = hour 0-23)
WEEKDAY_WEIGHTS = [
    0.10, 0.05, 0.05, 0.05, 0.05, 0.10,   # 0-5
    0.20, 0.40, 0.50, 0.50, 0.60, 0.80,   # 6-11
    1.50, 1.80, 1.20, 0.80, 0.70, 0.90,   # 12-17
    1.30, 2.00, 2.00, 1.50, 0.80, 0.30,   # 18-23
]
WEEKEND_WEIGHTS = [
    0.20, 0.10, 0.10, 0.10, 0.10, 0.10,
    0.20, 0.30, 0.50, 0.80, 1.00, 1.20,
    1.60, 1.80, 1.50, 1.20, 1.00, 1.10,
    1.40, 1.90, 1.90, 1.60, 1.00, 0.50,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_uuid(rng):
    """Generate a reproducible UUID v4 using the seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def epoch_ms(dt):
    """Convert a naive datetime (assumed UTC) to epoch milliseconds."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def random_coords(zone, rng):
    """Random lat/lon near zone center."""
    lat = zone["lat"] + rng.uniform(-zone["radius"], zone["radius"])
    lon = zone["lon"] + rng.uniform(-zone["radius"], zone["radius"])
    return round(lat, 6), round(lon, 6)


def zone_distance(z1, z2):
    """Euclidean distance between zone centers."""
    return math.sqrt((z1["lat"] - z2["lat"]) ** 2 + (z1["lon"] - z2["lon"]) ** 2)


def demand_multiplier(hour, is_weekend):
    """Returns demand weight for a given hour of the day."""
    weights = WEEKEND_WEIGHTS if is_weekend else WEEKDAY_WEIGHTS
    return weights[hour]


def is_restaurant_open(restaurant, hour):
    """Check if a restaurant is open at the given hour."""
    for start, end in restaurant["open_ranges"]:
        if start <= hour < end:
            return True
    return False


def sample_order_time(rng, base_date, is_weekend, surge_factor):
    """Sample a realistic order timestamp using rejection sampling."""
    max_weight = surge_factor * 2.5
    for _ in range(10000):
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        w = demand_multiplier(hour, is_weekend)
        if hour in range(12, 15) or hour in range(19, 23):
            w *= surge_factor
        if rng.uniform(0, max_weight) < w:
            return base_date.replace(hour=hour, minute=minute, second=second,
                                     microsecond=0)
    # Fallback (should not normally reach here)
    return base_date.replace(hour=12, minute=0, second=0, microsecond=0)


def sample_prep_time(restaurant, rng):
    """Sample prep time from per-restaurant normal distribution (Box-Muller)."""
    mean = restaurant["prep_mean"]
    std = restaurant["prep_std"]
    u1 = max(1e-10, rng.random())
    u2 = rng.random()
    z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    value = mean + std * z
    return max(180, int(value))  # minimum 3 minutes


# ---------------------------------------------------------------------------
# Entity builders
# ---------------------------------------------------------------------------

def build_zones(city, num_zones=None):
    """Build zone list from city preset."""
    templates = CITY_PRESETS.get(city, CITY_PRESETS["madrid"])
    if num_zones is not None:
        templates = templates[:num_zones]
    zones = []
    for name, lat, lon, radius, weight in templates:
        zones.append({
            "id": name, "lat": lat, "lon": lon,
            "radius": radius, "weight": weight,
        })
    total = sum(z["weight"] for z in zones)
    for z in zones:
        z["weight"] /= total
    return zones


def build_restaurants(n, zones, rng):
    """Build restaurants with zone assignment, opening hours, and prep profiles."""
    restaurants = []
    zone_ids = [z["id"] for z in zones]
    zone_weights = [z["weight"] for z in zones]
    profile_names = [p[0] for p in RESTAURANT_PROFILES]
    profile_weights = [p[2] for p in RESTAURANT_PROFILES]
    profile_map = {p[0]: p for p in RESTAURANT_PROFILES}

    for i in range(n):
        zone_id = rng.choices(zone_ids, weights=zone_weights)[0]
        profile_name = rng.choices(profile_names, weights=profile_weights)[0]
        profile = profile_map[profile_name]
        restaurants.append({
            "id": "rest_{:03d}".format(i + 1),
            "zone_id": zone_id,
            "profile": profile_name,
            "open_ranges": profile[1],
            "prep_mean": profile[3],
            "prep_std": profile[4],
        })
    return restaurants


def build_couriers(n, zones, rng):
    """Build couriers with initial zone assignment."""
    couriers = []
    zone_ids = [z["id"] for z in zones]
    zone_weights = [z["weight"] for z in zones]
    for i in range(n):
        zone_id = rng.choices(zone_ids, weights=zone_weights)[0]
        couriers.append({
            "id": "courier_{:03d}".format(i + 1),
            "zone_id": zone_id,
        })
    return couriers


def build_customers(n, rng):
    """Build customer ID list."""
    return ["customer_{:04d}".format(i + 1) for i in range(n)]


# ---------------------------------------------------------------------------
# Placement generation  (step 1: what orders are placed, when, and where)
# ---------------------------------------------------------------------------

def generate_placements(cfg, restaurants, customers, rng, base_date):
    """Generate base order placements (time, restaurant, customer)."""
    is_weekend = cfg["is_weekend"]
    placements = []
    for _ in range(cfg["num_orders"]):
        placed_dt = sample_order_time(rng, base_date, is_weekend, cfg["surge_factor"])
        hour = placed_dt.hour
        open_rests = [r for r in restaurants if is_restaurant_open(r, hour)]
        if not open_rests:
            open_rests = restaurants
        restaurant = rng.choice(open_rests)
        customer_id = rng.choice(customers)
        order_value = round(rng.uniform(8.0, 65.0), 2)
        promo = rng.random() < cfg["promo_prob"]
        if promo:
            order_value = round(order_value * rng.uniform(0.7, 0.9), 2)

        placements.append({
            "placed_dt": placed_dt,
            "customer_id": customer_id,
            "restaurant": restaurant,
            "order_value": order_value,
            "promo": promo,
            "force_cancel": False,
            "is_fraud": False,
            "is_surge": False,
        })
    return placements


def add_fraud_clusters(placements, cfg, customers, restaurants, rng, base_date, stats):
    """Add fraud cluster placements: bursts of cancellations from the same customer."""
    num_clusters = max(0, round(cfg["num_orders"] * cfg["fraud_cluster_prob"]))
    if num_clusters == 0:
        return

    for _ in range(num_clusters):
        customer_id = rng.choice(customers)
        peak_hour = rng.choice([12, 13, 19, 20, 21])
        base_minute = rng.randint(0, 45)
        cluster_size = rng.randint(3, 5)

        for j in range(cluster_size):
            open_rests = [r for r in restaurants if is_restaurant_open(r, peak_hour)]
            if not open_rests:
                open_rests = restaurants
            restaurant = rng.choice(open_rests)
            order_value = round(rng.uniform(8.0, 65.0), 2)
            placed_dt = base_date.replace(
                hour=peak_hour,
                minute=base_minute + rng.randint(0, 14),
                second=rng.randint(0, 59),
                microsecond=0,
            )
            placements.append({
                "placed_dt": placed_dt,
                "customer_id": customer_id,
                "restaurant": restaurant,
                "order_value": order_value,
                "promo": False,
                "force_cancel": True,
                "is_fraud": True,
                "is_surge": False,
            })
        stats["fraud_clusters_injected"] += 1


def add_zone_surge(placements, cfg, zones, restaurants, customers, rng, base_date, stats):
    """Add a burst of orders in one zone for a 15-minute window (5x demand)."""
    zone = rng.choice(zones)
    zone_id = zone["id"]
    peak_hour = rng.choice([12, 13, 19, 20, 21])
    base_minute = rng.randint(0, 45)
    num_surge = max(10, cfg["num_orders"] // 10)

    zone_rests = [r for r in restaurants if r["zone_id"] == zone_id]
    if not zone_rests:
        zone_rests = restaurants

    stats["zone_surge_zone"] = zone_id
    stats["zone_surge_hour"] = peak_hour
    stats["zone_surge_minute"] = base_minute
    stats["zone_surge_orders"] = num_surge

    for _ in range(num_surge):
        customer_id = rng.choice(customers)
        restaurant = rng.choice(zone_rests)
        order_value = round(rng.uniform(8.0, 65.0), 2)
        promo = rng.random() < cfg["promo_prob"]
        if promo:
            order_value = round(order_value * rng.uniform(0.7, 0.9), 2)
        placed_dt = base_date.replace(
            hour=peak_hour,
            minute=base_minute + rng.randint(0, 14),
            second=rng.randint(0, 59),
            microsecond=0,
        )
        placements.append({
            "placed_dt": placed_dt,
            "customer_id": customer_id,
            "restaurant": restaurant,
            "order_value": order_value,
            "promo": promo,
            "force_cancel": False,
            "is_fraud": False,
            "is_surge": True,
        })


# ---------------------------------------------------------------------------
# Courier assignment (zone-aware with availability tracking)
# ---------------------------------------------------------------------------

def assign_courier(courier_state, zone_id, zones, order_time_ms, rng):
    """
    Assign a courier to an order with zone-aware priority:
      1. Available couriers in the same zone
      2. Available couriers in the nearest zone
      3. Courier that becomes free soonest
    """
    zone_map = {z["id"]: z for z in zones}

    # Couriers available at or before this order's time
    available = []
    for cid, state in courier_state.items():
        if state["available_at"] <= order_time_ms:
            available.append((cid, state))

    if available:
        same_zone = [(cid, s) for cid, s in available if s["zone"] == zone_id]
        if same_zone:
            return rng.choice(same_zone)[0]

        order_zone = zone_map.get(zone_id)
        if order_zone:
            available.sort(key=lambda x: zone_distance(
                zone_map.get(x[1]["zone"], order_zone), order_zone
            ))
        return available[0][0]

    # No one available right now — pick the courier that frees up soonest
    soonest_cid = min(courier_state, key=lambda cid: courier_state[cid]["available_at"])
    return soonest_cid


# ---------------------------------------------------------------------------
# Order event processing  (step 2: turn placements into lifecycle events)
# ---------------------------------------------------------------------------

def process_placements(placements, cfg, zones, couriers, rng, stats):
    """
    Process sorted placements into order lifecycle events.
    Returns (order_events, courier_delivery_log, courier_state).
    """
    zone_map = {z["id"]: z for z in zones}
    order_events = []

    # Initialise courier state
    courier_state = {}
    for c in couriers:
        courier_state[c["id"]] = {
            "zone": c["zone_id"],
            "available_at": 0,
        }

    # Delivery log for courier event generation: courier_id -> list of deliveries
    courier_delivery_log = defaultdict(list)

    for pl in placements:
        placed_dt = pl["placed_dt"]
        customer_id = pl["customer_id"]
        restaurant = pl["restaurant"]
        zone_id = restaurant["zone_id"]
        order_value = pl["order_value"]
        promo = pl["promo"]
        force_cancel = pl["force_cancel"]

        order_id = new_uuid(rng)
        placed_ms = epoch_ms(placed_dt)
        app_version = "1.1.0" if rng.random() < 0.05 else "1.0.0"
        proc_delay = rng.randint(1000, 10000)

        # Stats tracking
        stats["orders_per_zone"][zone_id] += 1
        stats["orders_per_hour"][placed_dt.hour] += 1
        stats["order_values"].append(order_value)
        if pl["is_fraud"]:
            stats["fraud_order_events"] += 1
        if pl["is_surge"]:
            stats["surge_order_events"] += 1

        cancelled = force_cancel or (rng.random() < cfg["cancel_prob"])
        missing_pickup = (not cancelled) and (rng.random() < cfg["missing_step_prob"])
        impossible_duration = (not cancelled) and (rng.random() < cfg["impossible_duration_prob"])

        if missing_pickup:
            stats["missing_step_orders"] += 1
        if impossible_duration:
            stats["impossible_duration_orders"] += 1

        # --- ORDER_PLACED ---
        order_events.append({
            "event_id": new_uuid(rng),
            "order_id": order_id,
            "event_type": "ORDER_PLACED",
            "timestamp": placed_ms,
            "processing_timestamp": placed_ms + proc_delay,
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "courier_id": None,
            "zone_id": zone_id,
            "order_value": order_value,
            "promo_applied": promo,
            "cancellation_reason": None,
            "is_duplicate": False,
            "app_version": app_version,
        })

        if cancelled:
            cancel_dt = placed_dt + timedelta(seconds=rng.randint(30, 300))
            cancel_ms = epoch_ms(cancel_dt)
            reason = "customer_cancelled" if force_cancel else rng.choice(CANCELLATION_REASONS)
            order_events.append({
                "event_id": new_uuid(rng),
                "order_id": order_id,
                "event_type": "CANCELLED",
                "timestamp": cancel_ms,
                "processing_timestamp": cancel_ms + rng.randint(1000, 10000),
                "customer_id": customer_id,
                "restaurant_id": restaurant["id"],
                "courier_id": None,
                "zone_id": zone_id,
                "order_value": order_value,
                "promo_applied": promo,
                "cancellation_reason": reason,
                "is_duplicate": False,
                "app_version": app_version,
            })
            continue

        # --- COURIER_ASSIGNED ---
        courier_id = assign_courier(courier_state, zone_id, zones, placed_ms, rng)
        assign_dt = placed_dt + timedelta(seconds=rng.randint(30, 120))
        assign_ms = epoch_ms(assign_dt)

        order_events.append({
            "event_id": new_uuid(rng),
            "order_id": order_id,
            "event_type": "COURIER_ASSIGNED",
            "timestamp": assign_ms,
            "processing_timestamp": assign_ms + rng.randint(1000, 10000),
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "courier_id": courier_id,
            "zone_id": zone_id,
            "order_value": order_value,
            "promo_applied": promo,
            "cancellation_reason": None,
            "is_duplicate": False,
            "app_version": app_version,
        })

        # --- PICKED_UP ---
        prep_secs = sample_prep_time(restaurant, rng)
        if not missing_pickup:
            pickup_dt = assign_dt + timedelta(seconds=prep_secs)
            pickup_ms = epoch_ms(pickup_dt)
            order_events.append({
                "event_id": new_uuid(rng),
                "order_id": order_id,
                "event_type": "PICKED_UP",
                "timestamp": pickup_ms,
                "processing_timestamp": pickup_ms + rng.randint(1000, 10000),
                "customer_id": customer_id,
                "restaurant_id": restaurant["id"],
                "courier_id": courier_id,
                "zone_id": zone_id,
                "order_value": order_value,
                "promo_applied": promo,
                "cancellation_reason": None,
                "is_duplicate": False,
                "app_version": app_version,
            })
        else:
            pickup_dt = assign_dt + timedelta(seconds=rng.randint(300, 900))
            pickup_ms = epoch_ms(pickup_dt)

        # --- DELIVERED ---
        if impossible_duration:
            delivery_secs = rng.randint(1, 10)
        else:
            delivery_secs = rng.randint(600, 2400)
        delivered_dt = pickup_dt + timedelta(seconds=delivery_secs)
        delivered_ms = epoch_ms(delivered_dt)

        order_events.append({
            "event_id": new_uuid(rng),
            "order_id": order_id,
            "event_type": "DELIVERED",
            "timestamp": delivered_ms,
            "processing_timestamp": delivered_ms + rng.randint(1000, 10000),
            "customer_id": customer_id,
            "restaurant_id": restaurant["id"],
            "courier_id": courier_id,
            "zone_id": zone_id,
            "order_value": order_value,
            "promo_applied": promo,
            "cancellation_reason": None,
            "is_duplicate": False,
            "app_version": app_version,
        })

        # Update courier state: busy until delivery, stays in delivery zone
        courier_state[courier_id]["available_at"] = delivered_ms
        courier_state[courier_id]["zone"] = zone_id

        # Record delivery for courier event generation
        courier_delivery_log[courier_id].append({
            "order_id": order_id,
            "zone_id": zone_id,
            "assigned_ms": assign_ms,
            "pickup_ms": pickup_ms,
            "delivered_ms": delivered_ms,
        })

    return order_events, courier_delivery_log, courier_state


# ---------------------------------------------------------------------------
# Courier event generation
# ---------------------------------------------------------------------------

def generate_courier_events(cfg, zones, couriers, delivery_log, courier_state,
                            rng, base_date, stats):
    """Generate courier status events anchored to the delivery log."""
    events = []
    zone_map = {z["id"]: z for z in zones}

    for courier in couriers:
        cid = courier["id"]
        initial_zone_id = courier["zone_id"]
        zone = zone_map.get(initial_zone_id, zones[0])
        session_id = new_uuid(rng)
        app_version = "1.1.0" if rng.random() < 0.05 else "1.0.0"

        # Courier comes online at a random hour
        online_hour = rng.choice([10, 11, 17, 18, 19])
        online_dt = base_date.replace(
            hour=online_hour,
            minute=rng.randint(0, 59),
            second=rng.randint(0, 59),
            microsecond=0,
        )
        online_ms = epoch_ms(online_dt)
        lat, lon = random_coords(zone, rng)

        # ONLINE
        events.append({
            "event_id": new_uuid(rng),
            "courier_id": cid,
            "event_type": "ONLINE",
            "timestamp": online_ms,
            "processing_timestamp": online_ms + rng.randint(1000, 5000),
            "zone_id": zone["id"],
            "latitude": lat,
            "longitude": lon,
            "order_id": None,
            "session_id": session_id,
            "is_duplicate": False,
            "app_version": app_version,
        })

        # AVAILABLE right after coming online
        avail_dt = online_dt + timedelta(seconds=rng.randint(10, 60))
        avail_ms = epoch_ms(avail_dt)
        lat, lon = random_coords(zone, rng)
        events.append({
            "event_id": new_uuid(rng),
            "courier_id": cid,
            "event_type": "AVAILABLE",
            "timestamp": avail_ms,
            "processing_timestamp": avail_ms + rng.randint(1000, 5000),
            "zone_id": zone["id"],
            "latitude": lat,
            "longitude": lon,
            "order_id": None,
            "session_id": session_id,
            "is_duplicate": False,
            "app_version": app_version,
        })

        # Process deliveries assigned to this courier
        deliveries = sorted(delivery_log.get(cid, []), key=lambda x: x["assigned_ms"])
        went_offline_mid = False

        for i, delivery in enumerate(deliveries):
            dzone = zone_map.get(delivery["zone_id"], zone)

            # Mid-delivery offline edge case
            if (not went_offline_mid
                    and i == len(deliveries) // 2
                    and len(deliveries) > 0
                    and rng.random() < cfg["mid_delivery_offline_prob"]):
                offline_dt_raw = datetime.fromtimestamp(
                    delivery["assigned_ms"] / 1000, tz=timezone.utc
                ).replace(tzinfo=None)
                offline_dt = offline_dt_raw + timedelta(seconds=rng.randint(60, 300))
                offline_ms = epoch_ms(offline_dt)
                lat, lon = random_coords(dzone, rng)
                events.append({
                    "event_id": new_uuid(rng),
                    "courier_id": cid,
                    "event_type": "OFFLINE",
                    "timestamp": offline_ms,
                    "processing_timestamp": offline_ms + rng.randint(1000, 5000),
                    "zone_id": dzone["id"],
                    "latitude": lat,
                    "longitude": lon,
                    "order_id": delivery["order_id"],
                    "session_id": session_id,
                    "is_duplicate": False,
                    "app_version": app_version,
                })
                went_offline_mid = True
                stats["mid_delivery_offline"] += 1
                break

            # PICKING_UP
            lat, lon = random_coords(dzone, rng)
            events.append({
                "event_id": new_uuid(rng),
                "courier_id": cid,
                "event_type": "PICKING_UP",
                "timestamp": delivery["pickup_ms"],
                "processing_timestamp": delivery["pickup_ms"] + rng.randint(1000, 5000),
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": delivery["order_id"],
                "session_id": session_id,
                "is_duplicate": False,
                "app_version": app_version,
            })

            # DELIVERING
            lat, lon = random_coords(dzone, rng)
            mid_ts = (delivery["pickup_ms"] + delivery["delivered_ms"]) // 2
            events.append({
                "event_id": new_uuid(rng),
                "courier_id": cid,
                "event_type": "DELIVERING",
                "timestamp": mid_ts,
                "processing_timestamp": mid_ts + rng.randint(1000, 5000),
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": delivery["order_id"],
                "session_id": session_id,
                "is_duplicate": False,
                "app_version": app_version,
            })

            # AVAILABLE again after delivery — courier stays in delivery zone
            after_dt = datetime.fromtimestamp(
                delivery["delivered_ms"] / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
            after_dt += timedelta(seconds=rng.randint(30, 120))
            after_ms = epoch_ms(after_dt)
            lat, lon = random_coords(dzone, rng)
            events.append({
                "event_id": new_uuid(rng),
                "courier_id": cid,
                "event_type": "AVAILABLE",
                "timestamp": after_ms,
                "processing_timestamp": after_ms + rng.randint(1000, 5000),
                "zone_id": delivery["zone_id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": None,
                "session_id": session_id,
                "is_duplicate": False,
                "app_version": app_version,
            })
            # Update current zone reference for the next delivery
            zone = dzone

        # OFFLINE at end of shift (unless went offline mid-delivery)
        if not went_offline_mid:
            courier_events_for_cid = [e for e in events if e.get("courier_id") == cid]
            last_ts = max(
                (e["timestamp"] for e in courier_events_for_cid),
                default=online_ms,
            )
            offline_dt = datetime.fromtimestamp(
                last_ts / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
            offline_dt += timedelta(minutes=rng.randint(5, 30))
            offline_ms = epoch_ms(offline_dt)
            # Courier's final zone is whatever zone they last delivered to
            final_zone = zone_map.get(
                courier_state.get(cid, {}).get("zone", initial_zone_id),
                zone,
            )
            lat, lon = random_coords(final_zone, rng)
            events.append({
                "event_id": new_uuid(rng),
                "courier_id": cid,
                "event_type": "OFFLINE",
                "timestamp": offline_ms,
                "processing_timestamp": offline_ms + rng.randint(1000, 5000),
                "zone_id": final_zone["id"],
                "latitude": lat,
                "longitude": lon,
                "order_id": None,
                "session_id": session_id,
                "is_duplicate": False,
                "app_version": app_version,
            })

    # Stats: couriers per zone (based on initial assignment)
    for c in couriers:
        stats["couriers_per_zone"][c["zone_id"]] += 1

    return events


# ---------------------------------------------------------------------------
# Post-processing: duplicates & late events
# ---------------------------------------------------------------------------

def inject_duplicates(events, prob, rng, stats, feed_name):
    """Inject duplicate events (new event_id, is_duplicate=True)."""
    if prob <= 0 or not events:
        return
    num_dups = max(1, int(len(events) * prob))
    candidates = list(events)
    rng.shuffle(candidates)
    for e in candidates[:num_dups]:
        dup = dict(e)
        dup["event_id"] = new_uuid(rng)
        dup["is_duplicate"] = True
        dup["processing_timestamp"] = e["processing_timestamp"] + rng.randint(100, 5000)
        events.append(dup)
        stats["duplicates_injected_" + feed_name] += 1


def inject_late_events(events, prob, rng, stats, feed_name):
    """Shift timestamps backward to simulate late-arriving events."""
    if prob <= 0 or not events:
        return
    num_late = max(1, int(len(events) * prob))
    exclude = {"ORDER_PLACED", "ONLINE"}
    candidates = [e for e in events if e["event_type"] not in exclude]
    rng.shuffle(candidates)
    for e in candidates[:num_late]:
        e["timestamp"] -= rng.randint(300_000, 900_000)  # 5-15 min late
        # processing_timestamp stays — it represents actual ingestion time
        stats["late_events_" + feed_name] += 1


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------

def generate_validation_report(order_events, courier_events, stats, cfg):
    """Produce a comprehensive validation report."""
    report = {}

    # --- Event counts ---
    report["total_order_events"] = len(order_events)
    report["total_courier_events"] = len(courier_events)

    # --- Order event type breakdown ---
    order_types = defaultdict(int)
    for e in order_events:
        order_types[e["event_type"]] += 1
    total_oe = max(len(order_events), 1)
    report["order_event_breakdown"] = {
        k: {"count": v, "pct": round(v / total_oe * 100, 2)}
        for k, v in sorted(order_types.items())
    }

    # --- Courier event type breakdown ---
    courier_types = defaultdict(int)
    for e in courier_events:
        courier_types[e["event_type"]] += 1
    total_ce = max(len(courier_events), 1)
    report["courier_event_breakdown"] = {
        k: {"count": v, "pct": round(v / total_ce * 100, 2)}
        for k, v in sorted(courier_types.items())
    }

    # --- Edge case stats ---
    report["duplicates_injected"] = {
        "order": stats.get("duplicates_injected_order", 0),
        "courier": stats.get("duplicates_injected_courier", 0),
    }
    report["late_events_injected"] = {
        "order": stats.get("late_events_order", 0),
        "courier": stats.get("late_events_courier", 0),
    }
    report["missing_step_orders"] = stats.get("missing_step_orders", 0)
    report["impossible_duration_orders"] = stats.get("impossible_duration_orders", 0)
    report["mid_delivery_offline_couriers"] = stats.get("mid_delivery_offline", 0)
    report["fraud_clusters_injected"] = stats.get("fraud_clusters_injected", 0)
    report["fraud_cluster_order_events"] = stats.get("fraud_order_events", 0)

    # --- Zone surge ---
    if stats.get("zone_surge_orders", 0) > 0:
        report["zone_surge"] = {
            "zone": stats.get("zone_surge_zone", ""),
            "hour": stats.get("zone_surge_hour", 0),
            "minute_start": stats.get("zone_surge_minute", 0),
            "extra_orders": stats.get("zone_surge_orders", 0),
        }
    else:
        report["zone_surge"] = None

    # --- Order value stats ---
    values = stats.get("order_values", [])
    if values:
        report["order_value_stats"] = {
            "avg": round(sum(values) / len(values), 2),
            "min": round(min(values), 2),
            "max": round(max(values), 2),
        }
    else:
        report["order_value_stats"] = {"avg": 0, "min": 0, "max": 0}

    # --- Orders per zone ---
    opz = dict(stats.get("orders_per_zone", {}))
    total_orders_all = max(sum(opz.values()), 1)
    report["orders_per_zone"] = {
        k: {"count": v, "pct": round(v / total_orders_all * 100, 2)}
        for k, v in sorted(opz.items())
    }

    # --- Couriers per zone ---
    report["couriers_per_zone"] = dict(sorted(stats.get("couriers_per_zone", {}).items()))

    # --- Peak hour distribution ---
    oph = dict(stats.get("orders_per_hour", {}))
    report["orders_per_hour"] = {str(h): oph.get(h, 0) for h in range(24)}

    # --- Data quality warnings ---
    warnings = []
    cancel_count = order_types.get("CANCELLED", 0)
    placed_count = order_types.get("ORDER_PLACED", 0)
    if placed_count > 0 and cancel_count / placed_count > 0.25:
        warnings.append(
            "High cancellation rate: {:.1f}%".format(cancel_count / placed_count * 100)
        )
    dup_total = (stats.get("duplicates_injected_order", 0)
                 + stats.get("duplicates_injected_courier", 0))
    if dup_total == 0 and cfg.get("duplicate_prob", 0) > 0:
        warnings.append("No duplicates were injected despite duplicate_prob > 0")
    report["data_quality_warnings"] = warnings

    # --- Config snapshot ---
    report["config"] = {
        k: v for k, v in cfg.items()
        if k not in ("restaurants_list", "couriers_list")
    }

    return report


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def load_schema(path):
    """Load an AVRO schema from a .avsc file."""
    with open(path) as f:
        return json.load(f)


def write_json(events, path):
    """Write events as newline-delimited JSON."""
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print("  Written {} events -> {}".format(len(events), path))


def write_avro(events, schema_dict, path):
    """Write events to an AVRO file."""
    parsed = fastavro.parse_schema(schema_dict)
    with open(path, "wb") as f:
        fastavro.writer(f, parsed, events)
    print("  Written {} events -> {}".format(len(events), path))


# ---------------------------------------------------------------------------
# Streaming mode
# ---------------------------------------------------------------------------

def stream_events(order_events, courier_events, speed_factor):
    """
    Merge both feeds, sort by timestamp, and emit to stdout as NDJSON
    with realistic inter-event delays.

    speed_factor: N means 1 real second = N simulated seconds.
    Default 60 => 1 real second = 1 simulated minute.
    """
    tagged = []
    for e in order_events:
        rec = dict(e)
        rec["_feed"] = "order_events"
        tagged.append(rec)
    for e in courier_events:
        rec = dict(e)
        rec["_feed"] = "courier_events"
        tagged.append(rec)

    tagged.sort(key=lambda x: x["timestamp"])

    prev_ts = None
    for event in tagged:
        if prev_ts is not None:
            diff_ms = event["timestamp"] - prev_ts
            if diff_ms > 0 and speed_factor > 0:
                sleep_secs = (diff_ms / 1000.0) / speed_factor
                # Cap max sleep to avoid hanging on big gaps
                sleep_secs = min(sleep_secs, 5.0)
                if sleep_secs > 0.001:
                    time_module.sleep(sleep_secs)
        prev_ts = event["timestamp"]
        try:
            sys.stdout.write(json.dumps(event) + "\n")
            sys.stdout.flush()
        except BrokenPipeError:
            break


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(
        description="Food Delivery Streaming Event Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--num-orders", type=int, default=DEFAULTS["num_orders"],
                   help="Total orders to generate")
    p.add_argument("--num-couriers", type=int, default=DEFAULTS["num_couriers"],
                   help="Number of couriers")
    p.add_argument("--num-restaurants", type=int, default=DEFAULTS["num_restaurants"],
                   help="Number of restaurants")
    p.add_argument("--num-zones", type=int, default=5, choices=range(1, 6),
                   metavar="1-5", help="Number of geographic zones (1-5)")
    p.add_argument("--cancel-prob", type=float, default=DEFAULTS["cancel_prob"],
                   help="Cancellation probability (0-1)")
    p.add_argument("--promo-prob", type=float, default=DEFAULTS["promo_prob"],
                   help="Probability a promo discount is applied (0-1)")
    p.add_argument("--duplicate-prob", type=float, default=DEFAULTS["duplicate_prob"],
                   help="Probability of injecting duplicate events (0-1)")
    p.add_argument("--late-prob", type=float, default=DEFAULTS["late_prob"],
                   help="Probability of injecting late/out-of-order events (0-1)")
    p.add_argument("--missing-step-prob", type=float, default=DEFAULTS["missing_step_prob"],
                   help="Probability of skipping PICKED_UP step (0-1)")
    p.add_argument("--impossible-duration-prob", type=float,
                   default=DEFAULTS["impossible_duration_prob"],
                   help="Probability of anomalously fast delivery (0-1)")
    p.add_argument("--mid-delivery-offline-prob", type=float,
                   default=DEFAULTS["mid_delivery_offline_prob"],
                   help="Probability courier drops offline mid-delivery (0-1)")
    p.add_argument("--fraud-cluster-prob", type=float,
                   default=DEFAULTS["fraud_cluster_prob"],
                   help="Probability of injecting fraud cancellation clusters (0-1)")
    p.add_argument("--zone-surge-event", action="store_true", default=False,
                   help="Inject a 5x demand spike in one zone for 15 minutes")
    p.add_argument("--surge-factor", type=float, default=DEFAULTS["surge_factor"],
                   help="Demand multiplier during peak hours")
    p.add_argument("--city", type=str, default=DEFAULTS["city"],
                   choices=list(CITY_PRESETS.keys()),
                   help="City preset for zone coordinates")
    p.add_argument("--date", type=str, default=None,
                   help="Simulation date in YYYY-MM-DD format (default: today)")
    p.add_argument("--weekend", action="store_true", default=False,
                   help="Force weekend demand pattern regardless of date")
    p.add_argument("--stream", action="store_true", default=False,
                   help="Streaming mode: emit NDJSON to stdout with realistic delays")
    p.add_argument("--speed-factor", type=float, default=DEFAULTS["speed_factor"],
                   help="Streaming speed: 1 real second = N simulated seconds")
    p.add_argument("--output-dir", type=str, default=DEFAULTS["output_dir"],
                   help="Directory to write output files")
    p.add_argument("--seed", type=int, default=DEFAULTS["seed"],
                   help="Random seed for reproducibility")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def init_stats():
    """Initialise the statistics tracking dictionary."""
    return {
        "orders_per_zone": defaultdict(int),
        "orders_per_hour": defaultdict(int),
        "couriers_per_zone": defaultdict(int),
        "order_values": [],
        "duplicates_injected_order": 0,
        "duplicates_injected_courier": 0,
        "late_events_order": 0,
        "late_events_courier": 0,
        "missing_step_orders": 0,
        "impossible_duration_orders": 0,
        "mid_delivery_offline": 0,
        "fraud_clusters_injected": 0,
        "fraud_order_events": 0,
        "surge_order_events": 0,
        "zone_surge_orders": 0,
        "zone_surge_zone": "",
        "zone_surge_hour": 0,
        "zone_surge_minute": 0,
    }


def run_generator(args):
    """
    Core generation logic. Separated from main() so it can be called
    programmatically from tests.

    Returns (order_events, courier_events, report, stats).
    """
    rng = random.Random(args.seed)

    # Determine simulation date
    if args.date:
        base_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        base_date = datetime.now()
    base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
    is_weekend = args.weekend or base_date.weekday() >= 5

    cfg = {
        "num_orders": args.num_orders,
        "num_couriers": args.num_couriers,
        "num_restaurants": args.num_restaurants,
        "num_zones": args.num_zones,
        "cancel_prob": args.cancel_prob,
        "promo_prob": args.promo_prob,
        "duplicate_prob": args.duplicate_prob,
        "late_prob": args.late_prob,
        "missing_step_prob": args.missing_step_prob,
        "impossible_duration_prob": args.impossible_duration_prob,
        "mid_delivery_offline_prob": args.mid_delivery_offline_prob,
        "fraud_cluster_prob": args.fraud_cluster_prob,
        "zone_surge_event": args.zone_surge_event,
        "surge_factor": args.surge_factor,
        "city": args.city,
        "is_weekend": is_weekend,
        "seed": args.seed,
    }

    stats = init_stats()

    # Build entities
    zones = build_zones(cfg["city"], cfg["num_zones"])
    restaurants = build_restaurants(cfg["num_restaurants"], zones, rng)
    couriers_list = build_couriers(cfg["num_couriers"], zones, rng)
    customers = build_customers(max(50, cfg["num_orders"] // 3), rng)

    # Step 1: Generate base placements
    placements = generate_placements(cfg, restaurants, customers, rng, base_date)

    # Step 2: Add fraud clusters
    if cfg["fraud_cluster_prob"] > 0:
        add_fraud_clusters(placements, cfg, customers, restaurants, rng, base_date, stats)

    # Step 3: Add zone surge
    if cfg["zone_surge_event"]:
        add_zone_surge(placements, cfg, zones, restaurants, customers, rng, base_date, stats)

    # Step 4: Sort placements chronologically
    placements.sort(key=lambda x: x["placed_dt"])

    # Step 5: Process into order events (with zone-aware courier assignment)
    order_events, delivery_log, courier_state = process_placements(
        placements, cfg, zones, couriers_list, rng, stats,
    )

    # Step 6: Post-process order events
    inject_duplicates(order_events, cfg["duplicate_prob"], rng, stats, "order")
    inject_late_events(order_events, cfg["late_prob"], rng, stats, "order")
    rng.shuffle(order_events)

    # Step 7: Generate courier events
    courier_events = generate_courier_events(
        cfg, zones, couriers_list, delivery_log, courier_state, rng, base_date, stats,
    )
    inject_duplicates(courier_events, cfg["duplicate_prob"], rng, stats, "courier")
    inject_late_events(courier_events, cfg["late_prob"], rng, stats, "courier")
    rng.shuffle(courier_events)

    # Step 8: Validation report
    report = generate_validation_report(order_events, courier_events, stats, cfg)

    return order_events, courier_events, report, stats


def main():
    args = parse_args()
    order_events, courier_events, report, stats = run_generator(args)

    if args.stream:
        stream_events(order_events, courier_events, args.speed_factor)
        return

    # Write output files
    os.makedirs(args.output_dir, exist_ok=True)

    print("\nFood Delivery Event Generator")
    print("  Date        : {}".format(
        args.date or datetime.now().strftime("%Y-%m-%d")))
    print("  City        : {}".format(args.city))
    print("  Weekend     : {}".format(report["config"]["is_weekend"]))
    print("  Orders      : {}".format(args.num_orders))
    print("  Couriers    : {}".format(args.num_couriers))
    print("  Restaurants : {}".format(args.num_restaurants))
    print("  Zones       : {}".format(args.num_zones))
    print("  Seed        : {}".format(args.seed))
    print()

    # Load schemas
    script_dir = os.path.dirname(os.path.abspath(__file__))
    order_schema = load_schema(os.path.join(script_dir, "schemas", "order_event.avsc"))
    courier_schema = load_schema(os.path.join(script_dir, "schemas", "courier_event.avsc"))

    print("Writing output files...")
    write_json(order_events, os.path.join(args.output_dir, "order_events.json"))
    write_avro(order_events, order_schema, os.path.join(args.output_dir, "order_events.avro"))
    write_json(courier_events, os.path.join(args.output_dir, "courier_events.json"))
    write_avro(courier_events, courier_schema,
               os.path.join(args.output_dir, "courier_events.avro"))

    # Write validation report
    report_path = os.path.join(args.output_dir, "validation_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print("  Written validation report -> {}".format(report_path))

    # Print summary
    print("\nDone!")
    print("\nOrder event breakdown:   {}".format(
        dict(report["order_event_breakdown"])))
    print("Courier event breakdown: {}".format(
        dict(report["courier_event_breakdown"])))
    print("Duplicates injected:     {}".format(report["duplicates_injected"]))
    print("Late events injected:    {}".format(report["late_events_injected"]))
    print("Missing-step orders:     {}".format(report["missing_step_orders"]))
    print("Impossible-duration:     {}".format(report["impossible_duration_orders"]))
    print("Fraud clusters:          {}".format(report["fraud_clusters_injected"]))
    if report["zone_surge"]:
        print("Zone surge:              {}".format(report["zone_surge"]))
    print("\nAll files written to: {}/".format(args.output_dir))


if __name__ == "__main__":
    main()
