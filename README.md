# Real-Time Food Delivery Analytics Pipeline

A real-time streaming analytics system simulating the operational data of a food delivery
platform (similar to Uber Eats, Glovo, or Deliveroo). Built as part of a Big Data course
project exploring stream ingestion, processing, storage, and visualization.

---

## Project Overview

Modern food delivery platforms generate high-volume, high-velocity data streams: orders
being placed, couriers moving across a city, restaurants updating prep status, and
customers providing feedback. This project builds an end-to-end pipeline to ingest,
process, and visualize this "data in motion" in real time.

The system is designed around two core streaming feeds that capture the essential
supply-demand dynamics of the platform:

| Feed | Description |
|------|-------------|
| **Order Events** | Full lifecycle of every order: placed → assigned → picked up → delivered / cancelled |
| **Courier Events** | Real-time courier status: online → available → picking up → delivering → offline |

Together, these feeds allow us to answer the most important operational questions:
Are there enough couriers in each zone? Are deliveries taking too long? Where are
SLA breaches happening? Are there any fraud patterns?

---

## Team

| Name | Role |
|------|------|
| _(Team member 1)_ | _(e.g. Feed Design & Schema)_ |
| _(Team member 2)_ | _(e.g. Generator & Edge Cases)_ |
| _(Team member 3)_ | _(e.g. Stream Processing)_ |
| _(Team member 4)_ | _(e.g. Dashboard & Visualization)_ |

---

## Repository Structure

```
├── README.md                        # This file
├── generator/
│   ├── generator.py                 # Synthetic event generator
│   ├── README_generator.md          # Generator usage and design notes
│   └── schemas/
│       ├── order_event.avsc         # AVRO schema — Order Events
│       └── courier_event.avsc       # AVRO schema — Courier Events
├── sample_data/
│   ├── order_events.json            # Sample order events (newline-delimited JSON)
│   ├── order_events.avro            # Sample order events (AVRO binary)
│   ├── courier_events.json          # Sample courier events (newline-delimited JSON)
│   └── courier_events.avro          # Sample courier events (AVRO binary)
└── milestone2/                      # (Milestone 2 — to be completed)
    ├── ingestion/
    ├── processing/
    └── dashboard/
```

---

## Milestone 1 — Feed Design & Generation

### Why These Two Feeds?

**Order Events** are the heartbeat of the platform. Every revenue figure, SLA metric,
cancellation rate, and fraud signal originates from the order lifecycle. Without this feed,
there is no business analytics.

**Courier Events** represent the supply side of the marketplace. Pairing them with order
events allows us to compute demand-supply balance per zone in real time — the most
operationally critical metric for dispatch optimization and surge pricing.

Together they share three join keys (`order_id`, `courier_id`, `zone_id`) that enable
stream-stream and stream-table joins in Milestone 2.

### Analytics Enabled

| Use Case | Type | Feeds Used |
|----------|------|-----------|
| Average delivery time per zone (10-min tumbling window) | Basic | Order Events |
| Order throughput and cancellation rate over time | Basic | Order Events |
| Demand-supply health per zone (orders waiting vs couriers available) | Intermediate | Both |
| Courier active session duration and utilization | Intermediate | Courier Events |
| Restaurant SLA breach detection (prep time > threshold) | Intermediate | Order Events |
| Anomaly detection on delivery time spikes | Advanced | Both |
| Fraud heuristics: repeated cancellations from same customer | Advanced | Order Events |
| Surge zone prediction from recent window trends | Advanced | Both |

### Schema Design Notes

Both schemas are defined in AVRO format (`.avsc` files in `generator/schemas/`).

**Design decisions:**
- `timestamp` uses `long` with `logicalType: timestamp-millis` — the standard AVRO
  representation for event time, compatible with Spark's `from_unixtime` and watermarking.
- Optional fields (e.g. `courier_id`, `order_id`, `cancellation_reason`) use AVRO union
  type `["null", "string"]` with `"default": null`.
- `session_id` in the courier feed groups all events in one work shift, enabling
  session window aggregations in Milestone 2.
- `is_duplicate` flags synthetic test duplicates so downstream processors can validate
  deduplication without ambiguity.
- `schema_version` enables forward compatibility tracking as schemas evolve.

### Event-Time Processing Support

All events carry a `timestamp` field in epoch milliseconds representing the time
the event actually occurred (event time), not when it was ingested (processing time).
The generator deliberately injects:

- **Out-of-order events**: timestamps shifted back 5–15 minutes to simulate late arrivals
  via network delays or mobile reconnection
- **Duplicates**: same event re-emitted with a new `event_id` and `is_duplicate=True`
- **Missing steps**: orders jump from ASSIGNED to DELIVERED without PICKED_UP
- **Impossible durations**: delivery completed in under 10 seconds (anomaly injection)
- **Mid-delivery drop**: courier goes OFFLINE while an order is assigned to them

These edge cases are critical for validating watermark configuration and stateful
processing correctness in Milestone 2.

---

## Milestone 2 — Stream Processing, Storage & Dashboard

_(To be completed)_

- **Ingestion**: Azure Event Hubs (two topics, partitioned by zone_id)
- **Processing**: Spark Structured Streaming with tumbling/hopping/session windows
- **Storage**: Parquet on Azure Blob Storage
- **Dashboard**: Streamlit live dashboard

---

## Getting Started

### Prerequisites

```bash
pip install fastavro
```

### Generate Sample Data

```bash
cd generator
python generator.py --num-orders 200 --output-dir ../sample_data
```

See `generator/README_generator.md` for full configuration options.

---

## License

For academic use only — IE University Big Data course project.
