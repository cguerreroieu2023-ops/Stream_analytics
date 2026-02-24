# Design Document — Feed Design & Generation (Milestone 1)

This document formally justifies the technical decisions behind the synthetic data
generator for the real-time food delivery analytics pipeline.

---

## 1. Why These Two Feeds?

### Feed 1 — Order Events

Order events represent the **demand side** of the marketplace. Every revenue figure,
SLA metric, cancellation rate, and fraud signal originates from the order lifecycle.
We chose a full-lifecycle model (PLACED -> ASSIGNED -> PICKED_UP -> DELIVERED / CANCELLED)
because:

- **Windowed aggregations** (tumbling, hopping) over `timestamp` enable real-time KPIs
  like "average delivery time per zone in the last 10 minutes".
- **Stateful tracking** across event types lets us detect SLA breaches (e.g. prep time >
  threshold) and anomalies (impossibly fast deliveries).
- **Cancellation patterns** (same customer, same zone, short time window) serve as the
  basis for fraud detection in Milestone 2.

### Feed 2 — Courier Events

Courier events represent the **supply side**. They capture where couriers are, what
they are doing, and whether they are available for assignment. We chose a status-based
model (ONLINE -> AVAILABLE -> PICKING_UP -> DELIVERING -> OFFLINE) because:

- **Session windows** over `session_id` let us compute courier utilization and active
  time per shift.
- **Demand-supply balance** per zone (orders waiting vs couriers available) is the most
  operationally critical real-time metric. This requires joining both feeds on `zone_id`.
- **Location tracking** (`latitude`, `longitude`) enables geographic heat maps and
  courier redistribution alerts.

### Why Not More Feeds?

We deliberately limited the design to two feeds to keep the streaming pipeline
tractable for a course project while still enabling meaningful stream-stream joins,
windowed aggregations, and stateful processing. Additional feeds (e.g. restaurant
status, customer reviews) would add complexity without providing fundamentally new
streaming challenges.

---

## 2. How the Schemas Support Event-Time Processing and Late Data Handling

### Event Time vs Processing Time

Both schemas carry two timestamp fields:

| Field | Purpose |
|-------|---------|
| `timestamp` | **Event time** — when the event actually occurred in the real world |
| `processing_timestamp` | **Ingestion time** — when the event was received by the system |

This dual-timestamp design is critical for Milestone 2 because:

1. **Watermark configuration**: Spark Structured Streaming uses event time to advance
   watermarks. The gap between `timestamp` and `processing_timestamp` on late-arriving
   events determines how generous the watermark threshold must be.

2. **Late data testing**: The generator injects events whose `timestamp` is shifted
   backward by 5-15 minutes while their `processing_timestamp` stays near the original
   time. This simulates network delays and mobile reconnection scenarios.

3. **Correctness validation**: By comparing event-time windowed results with and
   without late data, we can verify that our watermark configuration correctly
   handles the expected lateness distribution.

### Schema Evolution Support

The `app_version` field simulates a rolling deployment where ~5% of events come from
a newer application version. This lets us test AVRO schema evolution and ensures
our pipeline can handle mixed-version events without failures.

Enum types include a `default` value for forward compatibility: if a future schema
version adds a new event type, existing consumers will gracefully fall back to the
default symbol rather than crashing.

### Join Keys

Both schemas share three join keys:
- `order_id` — links courier activity to specific orders (stream-stream join)
- `courier_id` — links orders to the courier handling them (stream-table join)
- `zone_id` — enables geographic aggregations across both feeds

---

## 3. How Each Edge Case Maps to a Streaming Challenge in Milestone 2

| Edge Case | Generator Flag | Streaming Challenge |
|-----------|---------------|-------------------|
| **Out-of-order events** | `--late-prob` | Tests watermark behaviour. If the watermark advances too aggressively, late events are dropped and window results become inaccurate. |
| **Duplicate events** | `--duplicate-prob` | Tests deduplication logic. Spark must use `dropDuplicates("event_id")` or equivalent to avoid double-counting. The `is_duplicate` flag allows validation. |
| **Missing lifecycle steps** | `--missing-step-prob` | Tests incomplete state handling. A stateful aggregation tracking order lifecycle must handle orders that skip from ASSIGNED to DELIVERED without PICKED_UP. |
| **Impossible durations** | `--impossible-duration-prob` | Tests anomaly detection. A delivery completed in <10 seconds is physically impossible and should trigger an alert or be filtered. |
| **Courier offline mid-delivery** | `--mid-delivery-offline-prob` | Tests stateful session logic. The session window for this courier ends abruptly while an order is still in-flight, requiring the pipeline to handle orphaned orders. |
| **Fraud clusters** | `--fraud-cluster-prob` | Tests pattern detection. A burst of 3-5 cancellations from the same customer in 15 minutes is a fraud signal that a CEP (Complex Event Processing) rule should detect. |
| **Zone surge** | `--zone-surge-event` | Tests surge detection and auto-scaling. A sudden 5x demand spike in one zone should trigger alerts and potentially redistribute couriers. |

---

## 4. Simulation Assumptions

### Time Model
- The simulation generates events for a **single day** (00:00 to 23:59).
- Order times follow a realistic hourly demand curve with lunch (12-14h) and dinner
  (19-22h) peaks. Weekend curves are flatter but with higher baseline demand.
- Restaurants have individual opening hours drawn from three profiles:
  all-day (50%), lunch+dinner (30%), dinner-only (20%).

### Geographic Model
- The city is divided into 5 zones with different demand weights (center gets ~30%
  of orders, periphery gets ~10%).
- Three city presets are available: Madrid, Barcelona, London — each with realistic
  zone coordinates.
- Couriers start in their assigned zone but **stay in the zone of their last delivery**
  after completing it (no teleportation back to home zone). This creates organic
  courier redistribution over time.

### Courier Assignment Model
- Courier assignment is **zone-aware**: the system prefers couriers currently in the
  same zone as the restaurant. If none are available, it falls back to the nearest zone.
- Courier availability is tracked throughout the simulation: a courier assigned to an
  order is marked as busy until the delivery is complete. This prevents double-assignment.

### Order Value Model
- Order values are sampled uniformly from EUR 8-65.
- If a promo is applied (configurable probability), the value is discounted by 10-30%.

### Randomness and Reproducibility
- All randomness flows through a single `random.Random` instance seeded with `--seed`.
- UUID generation uses `rng.getrandbits(128)` instead of `uuid.uuid4()` to ensure
  reproducibility.
- Running the generator twice with the same seed and parameters produces **identical**
  output, byte for byte. This is verified by the test suite.

### Scalability
- The generator handles up to 100,000 orders without memory issues.
- In streaming mode (`--stream`), events are emitted to stdout as NDJSON with
  configurable inter-event delays, suitable for piping into a Kafka/Event Hubs producer.
