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
# Default run: 200 orders, 20 couriers, 30 restaurants, 5 zones, Madrid
python generator.py

# Custom run with Barcelona preset and specific date
python generator.py \
  --num-orders 500 \
  --num-couriers 40 \
  --num-restaurants 60 \
  --city barcelona \
  --date 2026-03-01 \
  --cancel-prob 0.15 \
  --surge-factor 3.0 \
  --output-dir ./my_data \
  --seed 123

# Force weekend demand pattern
python generator.py --weekend --num-orders 300

# Enable all edge cases with custom probabilities
python generator.py \
  --num-orders 1000 \
  --duplicate-prob 0.10 \
  --late-prob 0.12 \
  --missing-step-prob 0.05 \
  --impossible-duration-prob 0.03 \
  --mid-delivery-offline-prob 0.08 \
  --fraud-cluster-prob 0.03 \
  --zone-surge-event

# Streaming mode: emit NDJSON to stdout
python generator.py --num-orders 500 --stream --speed-factor 60

# Pipe streaming output to a file
python generator.py --stream --speed-factor 30 > events.jsonl
```

Output files are written to `--output-dir` (default: `./sample_data`):

| File | Description |
|------|-------------|
| `order_events.json` | Newline-delimited JSON, one event per line |
| `order_events.avro` | Binary AVRO with embedded schema |
| `courier_events.json` | Newline-delimited JSON, one event per line |
| `courier_events.avro` | Binary AVRO with embedded schema |
| `validation_report.json` | Statistics and data quality report |

---

## Configuration Options

### Core Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--num-orders` | 200 | Total orders to simulate |
| `--num-couriers` | 20 | Number of active couriers |
| `--num-restaurants` | 30 | Number of restaurants |
| `--num-zones` | 5 | Geographic zones (1-5) |
| `--city` | madrid | City preset: `madrid`, `barcelona`, `london` |
| `--date` | today | Simulation date in YYYY-MM-DD format |
| `--weekend` | auto | Force weekend demand pattern |
| `--cancel-prob` | 0.10 | Probability an order is cancelled |
| `--promo-prob` | 0.20 | Probability a promo discount is applied |
| `--surge-factor` | 2.5 | Demand multiplier during peak hours |
| `--output-dir` | `./sample_data` | Directory for output files |
| `--seed` | 42 | Random seed for reproducibility |

### Edge Case Probabilities

| Flag | Default | Description |
|------|---------|-------------|
| `--duplicate-prob` | 0.05 | Probability of injecting a duplicate event |
| `--late-prob` | 0.08 | Probability of injecting a late/out-of-order event |
| `--missing-step-prob` | 0.03 | Probability of skipping PICKED_UP step |
| `--impossible-duration-prob` | 0.02 | Probability of anomalously fast delivery (<10s) |
| `--mid-delivery-offline-prob` | 0.05 | Probability courier goes offline mid-delivery |
| `--fraud-cluster-prob` | 0.02 | Probability of injecting fraud cancellation clusters |
| `--zone-surge-event` | off | Inject a 5x demand spike in one zone for 15 min |

### Streaming Mode

| Flag | Default | Description |
|------|---------|-------------|
| `--stream` | off | Enable streaming mode (NDJSON to stdout) |
| `--speed-factor` | 60 | 1 real second = N simulated seconds (60 = 1min) |

---

## Feed Designs

### Feed 1 — Order Events

Tracks the complete lifecycle of a food delivery order through five event types:

| Event Type | Description |
|-----------|-------------|
| `ORDER_PLACED` | Customer submits an order |
| `COURIER_ASSIGNED` | Platform assigns a courier (zone-aware) |
| `PICKED_UP` | Courier collects order from restaurant |
| `DELIVERED` | Order delivered to customer |
| `CANCELLED` | Order cancelled at any stage |

**Key fields:** `order_id` (joins all lifecycle events), `timestamp` (event-time),
`processing_timestamp` (ingestion-time for watermark testing),
`zone_id` + `restaurant_id` + `courier_id` (join keys),
`order_value` (revenue metric), `cancellation_reason` (fraud/ops analysis),
`app_version` (schema evolution testing).

### Feed 2 — Courier Status Events

Tracks courier availability and location through five status types:

| Event Type | Description |
|-----------|-------------|
| `ONLINE` | Courier starts a work session |
| `AVAILABLE` | Courier is idle and ready for assignment |
| `PICKING_UP` | Courier is travelling to the restaurant |
| `DELIVERING` | Courier is en route to the customer |
| `OFFLINE` | Courier ends their session |

**Key fields:** `session_id` (groups work shift events — supports session windows),
`courier_id` (joins to order feed), `latitude`/`longitude` (location tracking),
`order_id` (present during active deliveries), `processing_timestamp` (ingestion-time).

---

## Realism & Distribution

### Time Distribution
- Orders follow a realistic hourly demand curve with lunch peaks (12:00-14:00) and
  dinner peaks (19:00-22:00).
- **Weekday curves** have sharper peaks; **weekend curves** are flatter with higher
  baseline demand.
- The `--weekend` flag forces weekend mode; otherwise, it is auto-detected from `--date`.

### Restaurant Profiles
Each restaurant is randomly assigned one of three profiles:

| Profile | Probability | Opening Hours | Avg Prep Time |
|---------|------------|---------------|---------------|
| All-day | 50% | 10:00-23:00 | 15 min |
| Lunch+Dinner | 30% | 12:00-15:00, 19:00-23:00 | 12 min |
| Dinner-only | 20% | 18:00-23:00 | 20 min |

Orders are only generated when the chosen restaurant is open.
Prep times are sampled from a per-restaurant normal distribution.

### Geographic Model
- Three city presets with realistic zone coordinates:
  - **Madrid**: Center (Sol), North (Chamartin), South (Lavapies), East (Salamanca), West (Moncloa)
  - **Barcelona**: Eixample, Gothic, Gracia, Born, Sants
  - **London**: Soho, Shoreditch, Camden, Southbank, Kensington
- Five zones with different demand weights (center ~30%, periphery ~10%).
- **Zone-aware courier assignment**: couriers in the same zone as the restaurant are
  preferred. If none are available, the nearest-zone courier is assigned.
- After completing a delivery, couriers **stay in the delivery zone** (no teleportation).

### Order Values
- Sampled uniformly from EUR 8-65, reduced by 10-30% if a promo is applied.

---

## Edge Cases for Streaming Correctness

| Edge Case | How it's Generated | Streaming Challenge |
|-----------|-------------------|-------------------|
| **Out-of-order events** | Event timestamps shifted back 5-15 min | Tests watermark behaviour |
| **Duplicates** | `is_duplicate=True` events with new `event_id` | Tests deduplication logic |
| **Missing steps** | Orders go ASSIGNED -> DELIVERED without PICKED_UP | Tests incomplete state handling |
| **Impossible durations** | Delivery completed in <10 seconds | Tests anomaly detection |
| **Courier offline mid-delivery** | Courier goes OFFLINE while on a delivery | Tests stateful session logic |
| **Fraud clusters** | 3-5 cancellations from same customer in 15 min | Tests fraud pattern detection (CEP) |
| **Zone surge** | 5x demand spike in one zone for 15 min | Tests surge detection & auto-scaling |

All edge cases are configurable via CLI flags. Set probability to 0 to disable.

---

## Validation Report

After generation, `validation_report.json` contains:

- Total events per feed with type breakdown and percentages
- Number of duplicates, late events, missing-step orders injected
- Number of impossible-duration orders and mid-delivery offline couriers
- Number of fraud clusters injected
- Order value statistics (average, min, max)
- Orders per zone and couriers per zone with percentages
- Peak hour distribution (orders per hour, 0-23)
- Data quality warnings (e.g. unusually high cancellation rate)

---

## Docker

```bash
# Build image
docker build -t food-delivery-generator .

# Run with default settings
docker run -v $(pwd)/sample_data:/app/output food-delivery-generator

# Run with custom settings
docker-compose run generator --num-orders 1000 --stream --speed-factor 30

# Run with city preset
docker-compose run generator --num-orders 500 --city london --date 2026-03-15
```

---

## Testing

```bash
pip install pytest
pytest tests/test_generator.py -v
```

The test suite validates:
- Every order has an ORDER_PLACED event
- DELIVERED orders always have COURIER_ASSIGNED
- CANCELLED orders never have PICKED_UP or DELIVERED
- Timestamps are monotonically increasing (except intentional late events)
- Duplicates have `is_duplicate=True` and share `order_id` with originals
- All events validate against their AVRO schema
- Fraud clusters produce the expected pattern
- Same `--seed` produces identical output (reproducibility)
