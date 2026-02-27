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
| **Order Events** | Full lifecycle of every order: placed -> assigned -> picked up -> delivered / cancelled |
| **Courier Events** | Real-time courier status: online -> available -> picking up -> delivering -> offline |

Together, these feeds allow us to answer the most important operational questions:
Are there enough couriers in each zone? Are deliveries taking too long? Where are
SLA breaches happening? Are there any fraud patterns?

---

## Team

| Name | Role |
|------|------|
| Roberto Ahumada Gomez | Feed Design & Schema |
| Claudia Guerrero Morales | Generator & Edge Cases |
| Omar Ramzi F. Majdalawi | Stream Processing |
| Sofia Melendez Cabero | Dashboard & Visualization |
| Bruno Monroy Ortiz | Data Ingestion & Pipeline |
| Juancho Mulato Yuvienco | Testing & Documentation |

---

## Repository Structure

```
├── README.md                        # This file, overview of the project
├── DESIGN.md                        # Design justifications & assumptions
├── Dockerfile                       # Docker image for the generator
├── docker-compose.yml               # Docker Compose for easy execution
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
│   ├── courier_events.avro          # Sample courier events (AVRO binary)
│   └── validation_report.json       # Data quality & statistics report
├── tests/
│   └── test_generator.py            # Comprehensive pytest test suite
└── milestone2/                      # milestone 2 will be completed for the following submission
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

See [DESIGN.md](DESIGN.md) for the full formal justification.

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

### Schema Design

Both schemas are defined in AVRO format (`.avsc` files in `generator/schemas/`).

**Key design decisions:**
- `timestamp` (event time) and `processing_timestamp` (ingestion time) are both
  `long` with `logicalType: timestamp-millis` — enabling watermark and lateness testing.
- Optional fields use AVRO union type `["null", "string"]` with `"default": null`.
- `session_id` in the courier feed groups all events in one work shift for session windows.
- `is_duplicate` flags synthetic test duplicates for deduplication validation.
- `app_version` simulates schema evolution scenarios (mixed app versions in the stream).
- Enum types include a `default` value for forward compatibility.

### Edge Cases for Streaming Correctness

| Edge Case | Flag | Default | Purpose |
|-----------|------|---------|---------|
| Out-of-order events | `--late-prob` | 0.08 | Tests watermark behaviour |
| Duplicates | `--duplicate-prob` | 0.05 | Tests deduplication logic |
| Missing steps | `--missing-step-prob` | 0.03 | Tests incomplete state handling |
| Impossible durations | `--impossible-duration-prob` | 0.02 | Tests anomaly detection |
| Courier offline mid-delivery | `--mid-delivery-offline-prob` | 0.05 | Tests stateful session logic |
| Fraud clusters | `--fraud-cluster-prob` | 0.02 | Tests fraud pattern detection |
| Zone surge | `--zone-surge-event` | off | Tests surge detection |

### Validation Report

After generation, a `validation_report.json` is produced with comprehensive statistics:
event counts, type breakdowns, edge case counts, order value stats, per-zone distributions,
hourly demand curves, and data quality warnings.

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
pip install fastavro pytest
```

### Generate Sample Data

```bash
cd generator
python generator.py --num-orders 200 --output-dir ../sample_data --date 2026-02-24
```

### Run with Docker

```bash
# Build and run
docker-compose run generator --num-orders 1000

# Streaming mode
docker-compose run generator --num-orders 1000 --stream --speed-factor 30

# Custom city and date
docker-compose run generator --num-orders 500 --city barcelona --date 2026-03-01 --weekend
```

### Run Tests

```bash
pytest tests/test_generator.py -v
```

### Streaming Mode

```bash
# Emit events to stdout as NDJSON with 60x speed (1 real sec = 1 simulated min)
python generator/generator.py --num-orders 500 --stream --speed-factor 60

# Pipe to a file or Kafka producer
python generator/generator.py --stream | kafka-console-producer --topic orders
```

See `generator/README_generator.md` for full configuration options.

---

## License

For academic use only — IE University Big Data course project.
