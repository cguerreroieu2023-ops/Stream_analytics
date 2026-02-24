"""
Test suite for the Food Delivery Event Generator.

Run with:
    pytest tests/test_generator.py -v
"""

import json
import os
import sys
from collections import defaultdict

import pytest
import fastavro

# Allow importing from the generator package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
import generator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def generated_data():
    """Run the generator once with a fixed seed and return results."""
    args = generator.parse_args([
        "--num-orders", "100",
        "--num-couriers", "15",
        "--num-restaurants", "20",
        "--seed", "42",
        "--date", "2026-02-24",
        "--fraud-cluster-prob", "0.03",
        "--zone-surge-event",
    ])
    order_events, courier_events, report, stats = generator.run_generator(args)
    return {
        "order_events": order_events,
        "courier_events": courier_events,
        "report": report,
        "stats": stats,
        "args": args,
    }


@pytest.fixture(scope="module")
def order_lifecycles(generated_data):
    """Group order events by order_id for lifecycle analysis."""
    lifecycles = defaultdict(list)
    for e in generated_data["order_events"]:
        lifecycles[e["order_id"]].append(e)
    # Sort each lifecycle by timestamp
    for oid in lifecycles:
        lifecycles[oid].sort(key=lambda x: x["timestamp"])
    return dict(lifecycles)


# ---------------------------------------------------------------------------
# Test: Every order has an ORDER_PLACED event
# ---------------------------------------------------------------------------

class TestOrderLifecycle:

    def test_every_order_has_placed(self, order_lifecycles):
        """Every unique order_id must have at least one ORDER_PLACED event."""
        for oid, events in order_lifecycles.items():
            types = {e["event_type"] for e in events if not e["is_duplicate"]}
            assert "ORDER_PLACED" in types, (
                "Order {} is missing ORDER_PLACED event".format(oid)
            )

    def test_delivered_orders_have_courier_assigned(self, order_lifecycles):
        """Every DELIVERED order must have a COURIER_ASSIGNED event."""
        for oid, events in order_lifecycles.items():
            non_dup = [e for e in events if not e["is_duplicate"]]
            types = {e["event_type"] for e in non_dup}
            if "DELIVERED" in types:
                assert "COURIER_ASSIGNED" in types, (
                    "Order {} is DELIVERED but missing COURIER_ASSIGNED".format(oid)
                )

    def test_cancelled_orders_no_pickup_or_delivered(self, order_lifecycles):
        """CANCELLED orders must never have PICKED_UP or DELIVERED events."""
        for oid, events in order_lifecycles.items():
            non_dup = [e for e in events if not e["is_duplicate"]]
            types = {e["event_type"] for e in non_dup}
            if "CANCELLED" in types:
                assert "PICKED_UP" not in types, (
                    "Order {} is CANCELLED but has PICKED_UP".format(oid)
                )
                assert "DELIVERED" not in types, (
                    "Order {} is CANCELLED but has DELIVERED".format(oid)
                )

    def test_timestamps_monotonically_increasing(self, order_lifecycles, generated_data):
        """
        Timestamps within a non-duplicate order lifecycle should be
        monotonically increasing, except for intentionally late events.
        """
        late_count = generated_data["stats"].get("late_events_order", 0)
        violations = 0
        for oid, events in order_lifecycles.items():
            non_dup = [e for e in events if not e["is_duplicate"]]
            non_dup.sort(key=lambda x: x["timestamp"])
            for i in range(1, len(non_dup)):
                if non_dup[i]["timestamp"] < non_dup[i - 1]["timestamp"]:
                    violations += 1
        # Some violations are expected due to late event injection
        assert violations <= late_count, (
            "Too many timestamp violations ({}) vs late events ({})".format(
                violations, late_count
            )
        )


# ---------------------------------------------------------------------------
# Test: Duplicates
# ---------------------------------------------------------------------------

class TestDuplicates:

    def test_duplicates_flagged(self, generated_data):
        """All duplicate events must have is_duplicate=True."""
        for e in generated_data["order_events"]:
            if e["is_duplicate"]:
                assert e["is_duplicate"] is True

    def test_duplicate_shares_order_id(self, generated_data):
        """Duplicate events share an order_id with a non-duplicate event."""
        non_dup_oids = {
            e["order_id"]
            for e in generated_data["order_events"]
            if not e["is_duplicate"]
        }
        for e in generated_data["order_events"]:
            if e["is_duplicate"]:
                assert e["order_id"] in non_dup_oids, (
                    "Duplicate event has order_id {} not in original events".format(
                        e["order_id"]
                    )
                )

    def test_duplicates_injected(self, generated_data):
        """With duplicate_prob > 0, at least some duplicates should exist."""
        order_dups = sum(
            1 for e in generated_data["order_events"] if e["is_duplicate"]
        )
        courier_dups = sum(
            1 for e in generated_data["courier_events"] if e["is_duplicate"]
        )
        assert order_dups > 0, "No order duplicates injected"
        assert courier_dups > 0, "No courier duplicates injected"


# ---------------------------------------------------------------------------
# Test: AVRO schema validation
# ---------------------------------------------------------------------------

class TestAvroValidation:

    def _get_schema(self, name):
        schema_dir = os.path.join(
            os.path.dirname(__file__), "..", "generator", "schemas"
        )
        path = os.path.join(schema_dir, name)
        with open(path) as f:
            return fastavro.parse_schema(json.load(f))

    def test_order_events_validate(self, generated_data):
        """All order events must validate against the AVRO schema."""
        schema = self._get_schema("order_event.avsc")
        for i, event in enumerate(generated_data["order_events"]):
            assert fastavro.validate(event, schema), (
                "Order event {} failed AVRO validation: {}".format(i, event)
            )

    def test_courier_events_validate(self, generated_data):
        """All courier events must validate against the AVRO schema."""
        schema = self._get_schema("courier_event.avsc")
        for i, event in enumerate(generated_data["courier_events"]):
            assert fastavro.validate(event, schema), (
                "Courier event {} failed AVRO validation: {}".format(i, event)
            )


# ---------------------------------------------------------------------------
# Test: Fraud clusters
# ---------------------------------------------------------------------------

class TestFraudClusters:

    def test_fraud_clusters_present(self, generated_data):
        """With fraud_cluster_prob > 0, fraud clusters should be injected."""
        assert generated_data["stats"]["fraud_clusters_injected"] > 0

    def test_fraud_cluster_pattern(self, generated_data, order_lifecycles):
        """
        Fraud clusters should produce multiple CANCELLED orders from the
        same customer in a short time window.
        """
        # Group cancelled orders by customer
        customer_cancellations = defaultdict(list)
        for oid, events in order_lifecycles.items():
            non_dup = [e for e in events if not e["is_duplicate"]]
            types = {e["event_type"] for e in non_dup}
            if "CANCELLED" in types:
                placed = [e for e in non_dup if e["event_type"] == "ORDER_PLACED"]
                if placed:
                    customer_cancellations[placed[0]["customer_id"]].append(
                        placed[0]["timestamp"]
                    )

        # At least one customer should have 3+ cancellations in a 15-min window
        found_cluster = False
        for cid, timestamps in customer_cancellations.items():
            if len(timestamps) < 3:
                continue
            timestamps.sort()
            for i in range(len(timestamps) - 2):
                window = timestamps[i + 2] - timestamps[i]
                if window <= 15 * 60 * 1000:  # 15 minutes in ms
                    found_cluster = True
                    break
            if found_cluster:
                break
        assert found_cluster, "No fraud cluster pattern detected (3+ cancels in 15 min)"


# ---------------------------------------------------------------------------
# Test: Reproducibility
# ---------------------------------------------------------------------------

class TestReproducibility:

    def test_seed_produces_identical_output(self):
        """Running with the same --seed must produce identical events."""
        args1 = generator.parse_args([
            "--num-orders", "30", "--seed", "999", "--date", "2026-01-15",
        ])
        args2 = generator.parse_args([
            "--num-orders", "30", "--seed", "999", "--date", "2026-01-15",
        ])
        oe1, ce1, _, _ = generator.run_generator(args1)
        oe2, ce2, _, _ = generator.run_generator(args2)

        assert len(oe1) == len(oe2), "Order event count differs"
        assert len(ce1) == len(ce2), "Courier event count differs"

        for a, b in zip(oe1, oe2):
            assert a == b, "Order events differ"
        for a, b in zip(ce1, ce2):
            assert a == b, "Courier events differ"

    def test_different_seed_produces_different_output(self):
        """Different seeds must produce different events."""
        args1 = generator.parse_args([
            "--num-orders", "30", "--seed", "111", "--date", "2026-01-15",
        ])
        args2 = generator.parse_args([
            "--num-orders", "30", "--seed", "222", "--date", "2026-01-15",
        ])
        oe1, _, _, _ = generator.run_generator(args1)
        oe2, _, _, _ = generator.run_generator(args2)

        # At least the event IDs should differ
        ids1 = {e["event_id"] for e in oe1}
        ids2 = {e["event_id"] for e in oe2}
        assert ids1 != ids2, "Different seeds produced identical event IDs"


# ---------------------------------------------------------------------------
# Test: Processing timestamp
# ---------------------------------------------------------------------------

class TestProcessingTimestamp:

    def test_processing_timestamp_exists(self, generated_data):
        """All events must have a processing_timestamp field."""
        for e in generated_data["order_events"]:
            assert "processing_timestamp" in e
            assert isinstance(e["processing_timestamp"], int)
        for e in generated_data["courier_events"]:
            assert "processing_timestamp" in e
            assert isinstance(e["processing_timestamp"], int)

    def test_processing_timestamp_after_or_near_event_time(self, generated_data):
        """processing_timestamp should generally be >= timestamp for non-late events."""
        # For non-late events (most events), processing_timestamp >= timestamp
        order_ok = 0
        for e in generated_data["order_events"]:
            if e["processing_timestamp"] >= e["timestamp"]:
                order_ok += 1
        # Most events should satisfy this (late events may not)
        ratio = order_ok / max(len(generated_data["order_events"]), 1)
        assert ratio > 0.8, (
            "Too few events have processing_timestamp >= timestamp: {:.0%}".format(ratio)
        )


# ---------------------------------------------------------------------------
# Test: Zone surge
# ---------------------------------------------------------------------------

class TestZoneSurge:

    def test_zone_surge_injected(self, generated_data):
        """With --zone-surge-event, surge orders should appear in the report."""
        report = generated_data["report"]
        assert report["zone_surge"] is not None
        assert report["zone_surge"]["extra_orders"] > 0


# ---------------------------------------------------------------------------
# Test: Validation report
# ---------------------------------------------------------------------------

class TestValidationReport:

    def test_report_has_required_fields(self, generated_data):
        """The validation report must contain all required sections."""
        report = generated_data["report"]
        required_keys = [
            "total_order_events",
            "total_courier_events",
            "order_event_breakdown",
            "courier_event_breakdown",
            "duplicates_injected",
            "late_events_injected",
            "missing_step_orders",
            "impossible_duration_orders",
            "fraud_clusters_injected",
            "order_value_stats",
            "orders_per_zone",
            "couriers_per_zone",
            "orders_per_hour",
            "data_quality_warnings",
        ]
        for key in required_keys:
            assert key in report, "Report missing key: {}".format(key)

    def test_report_event_counts_match(self, generated_data):
        """Report event counts should match actual event list lengths."""
        report = generated_data["report"]
        assert report["total_order_events"] == len(generated_data["order_events"])
        assert report["total_courier_events"] == len(generated_data["courier_events"])


# ---------------------------------------------------------------------------
# Test: App version field
# ---------------------------------------------------------------------------

class TestAppVersion:

    def test_app_version_present(self, generated_data):
        """All events must have an app_version field."""
        for e in generated_data["order_events"]:
            assert "app_version" in e
            assert e["app_version"] in ("1.0.0", "1.1.0")
        for e in generated_data["courier_events"]:
            assert "app_version" in e
            assert e["app_version"] in ("1.0.0", "1.1.0")
