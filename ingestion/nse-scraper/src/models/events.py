"""Event envelope models for consistent event structure."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass
class EventEnvelope:
    """Standard event envelope for all Kafka events.

    Follows the event-driven architecture principles:
    - Immutable events with unique IDs
    - Explicit timestamps for event_time vs ingest_time
    - Source attribution and schema versioning
    - Entity ID for Kafka partitioning
    """

    event_id: str
    event_time: int  # milliseconds since epoch
    ingest_time: int  # milliseconds since epoch
    source: str
    schema_version: str
    entity_id: str
    payload: dict[str, Any]

    @staticmethod
    def create(
        entity_id: str,
        source: str,
        payload: dict[str, Any],
        event_time: datetime | None = None,
        schema_version: str = "v1",
        event_id: str | None = None,
    ) -> "EventEnvelope":
        """Create a new event envelope.

        Args:
            entity_id: Entity identifier for partitioning (e.g., 'RELIANCE:NSE')
            source: Data source identifier (e.g., 'nse_cm_bhavcopy')
            payload: Event payload data
            event_time: When the event occurred (defaults to now)
            schema_version: Schema version (defaults to 'v1')
            event_id: Optional explicit event ID (generates UUID if not provided)

        Returns:
            EventEnvelope instance
        """
        now = datetime.now(UTC)
        event_dt = event_time or now

        # Ensure event_dt is timezone-aware (treat naive datetimes as UTC)
        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=UTC)

        return EventEnvelope(
            event_id=event_id or str(uuid.uuid4()),
            event_time=int(event_dt.timestamp() * 1000),
            ingest_time=int(now.timestamp() * 1000),
            source=source,
            schema_version=schema_version,
            entity_id=entity_id,
            payload=payload,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert envelope to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        return {
            "event_id": self.event_id,
            "event_time": self.event_time,
            "ingest_time": self.ingest_time,
            "source": self.source,
            "schema_version": self.schema_version,
            "entity_id": self.entity_id,
            "payload": self.payload,
        }


def create_deterministic_event_id(source: str, *keys: str) -> str:
    """Create deterministic event ID using UUID5.

    This ensures idempotency - the same input always produces the same event_id,
    allowing downstream systems to detect and handle duplicate events.

    Args:
        source: Source identifier (e.g., 'nse_cm_bhavcopy')
        *keys: Additional keys to make the ID unique (e.g., date, symbol)

    Returns:
        Deterministic UUID string
    """
    namespace = uuid.NAMESPACE_DNS
    composite_key = f"{source}:{':'.join(str(k) for k in keys)}"
    return str(uuid.uuid5(namespace, composite_key))
