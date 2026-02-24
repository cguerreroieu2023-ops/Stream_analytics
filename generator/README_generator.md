# Food Delivery Event Generator

Synthetic streaming data generator for a real-time food delivery platform.
Produces two event feeds — **Order Events** and **Courier Events** — in both
JSON (newline-delimited) and AVRO formats.

---

## Requirements

```
Python 3.8+
fastavro>=1.7
```

Install dependencies:

```bash
pip install fastavro
```

---

## Quick Start

```bash
# Default run: 200 orders, 20 couriers, 30 restaurants, 5 zones
python generator.py

# Custom run
python generator.py \
  --num-orders 500 \
  --num-couriers 40 \
  --num-restaurants 60 \
  --cancel-prob 0.15 \
  --surge-factor 3.0 \
  --output-dir ./my_data \
  --seed 123
```

Output files are written to `--output-dir` (default: `./sample_data`):

| File | Description |
|------|-------------|
| `order_events.json` | Newline-delimited JSON, one event per line |
| `order_events.avro` | Binary AVRO with embedded schema |
| `courier_events.json` | Newline-delimited JSON, one event per line |
| `courier_events.avro` | Binary AVRO with embedded schema |

---

## Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `--num-orders` | 200 | Total orders to simulate |
| `--num-couriers` | 20 | Number of active couriers |
| `--num-restaurants` | 30 | Number of restaurants |
| `--num-zones` | 5 | Geographic zones (1–5) |
| `--cancel-prob` | 0.10 | Probability an order is cancelled |
| `--promo-prob` | 0.20 | Probability a promo discount is applied |
| `--duplicate-prob` | 0.05 | Probability of injecting a duplicate event |
| `--late-prob` | 0.08 | Probability of injecting a late/out-of-order event |
| `--surge-factor` | 2.5 | Demand multiplier during peak hours |
| `--output-dir` | `./sample_data` | Directory for output files |
| `--seed` | 42 | Random seed for reproducibility |

---

## Feed Designs

### Feed 1 — Order Events

Tracks the complete lifecycle of a food delivery order through five event types:

| Event Type | Description |
|-----------|-------------|
| `ORDER_PLACED` | Customer submits an order |
| `COURIER_ASSIGNED` | Platform assigns a courier |
| `PICKED_UP` | Courier collects order from restaurant |
| `DELIVERED` | Order delivered to customer |
| `CANCELLED` | Order cancelled at any stage |

**Key fields:** `order_id` (joins all lifecycle events), `timestamp` (event-time for windowing),
`zone_id` + `restaurant_id` + `courier_id` (join keys for stream-table joins),
`order_value` (revenue metric), `cancellation_reason` (fraud/ops analysis).

### Feed 2 — Courier Status Events

Tracks courier availability and location through five status types:

| Event Type | Description |
|-----------|-------------|
| `ONLINE` | Courier starts a work session |
| `AVAILABLE` | Courier is idle and ready for assignment |
| `PICKING_UP` | Courier is travelling to the restaurant |
| `DELIVERING` | Courier is en route to the customer |
| `OFFLINE` | Courier ends their session |

**Key fields:** `session_id` (groups all events in one work shift — supports session windows),
`courier_id` (joins to order feed), `latitude`/`longitude` (location tracking),
`order_id` (optional — present during active deliveries).

---

## Realism & Distribution

- **Time distribution**: Orders follow a realistic hourly demand curve with lunch peaks
  (12:00–14:00) and dinner peaks (19:00–22:00). Weekends have a flatter but elevated curve.
- **Zone skew**: Five geographic zones have different demand weights (zone_center gets ~30%
  of traffic vs zone_west at ~10%).
- **Surge hours**: Peak windows apply `--surge-factor` as a demand multiplier.
- **Order values**: Sampled from a uniform distribution (€8–€65), reduced if a promo applies.

---

## Edge Cases for Streaming Correctness

The generator deliberately injects the following edge cases:

| Edge Case | How it's Generated | Purpose |
|-----------|-------------------|---------|
| **Out-of-order events** | Event timestamps shifted back 5–15 min after shuffle | Tests watermark behaviour |
| **Duplicates** | `is_duplicate=True` events with new `event_id` | Tests deduplication logic |
| **Missing steps** | ~3% of orders go `ASSIGNED → DELIVERED` without `PICKED_UP` | Tests incomplete state handling |
| **Impossible durations** | ~2% of orders delivered in <10 seconds | Tests anomaly detection |
| **Courier goes offline mid-delivery** | ~5% chance per courier session | Tests stateful session logic |

All edge cases can be controlled via probabilities (`--duplicate-prob`, `--late-prob`).
Set them to 0 to disable.

---

## File Structure

```
food_delivery_generator/
├── generator.py              # Main generator script
├── README_generator.md       # This file
├── schemas/
│   ├── order_event.avsc      # AVRO schema for order events
│   └── courier_event.avsc    # AVRO schema for courier events
└── sample_data/
    ├── order_events.json
    ├── order_events.avro
    ├── courier_events.json
    └── courier_events.avro
```
