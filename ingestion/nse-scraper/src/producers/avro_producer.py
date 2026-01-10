"""Avro producer for Kafka with Schema Registry integration."""

from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from src.config import config
from src.models.schemas import get_schema_loader
from src.utils.logger import get_logger
from src.utils.metrics import kafka_produce_failed, kafka_produce_success

logger = get_logger(__name__)


class AvroProducer:
    """Kafka producer with Avro serialization and Schema Registry integration."""

    def __init__(self, topic: str, schema_type: str = "raw_equity_ohlc"):
        """Initialize Avro producer.

        Args:
            topic: Kafka topic name
            schema_type: Schema type to load (e.g., 'raw_equity_ohlc', 'symbol_master')
        """
        self.topic = topic
        self.schema_type = schema_type
        self.logger = get_logger(f"{__name__}.{topic}")

        # Load Avro schema
        schema_loader = get_schema_loader()
        self.value_schema = schema_loader.load_value_schema(schema_type)
        self.logger.info(
            "Loaded Avro schema",
            schema_type=schema_type,
            schema_name=self.value_schema.get("name"),
        )

        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient(
            {"url": config.kafka.schema_registry_url}
        )

        # Create Avro serializer - need to pass schema as JSON string
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=schema_loader.get_schema_string(schema_loader.schema_map()[schema_type]),
            to_dict=lambda obj, ctx: obj,  # Pass dict directly
        )

        # Configure Kafka producer
        producer_config = {
            "bootstrap.servers": config.kafka.bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
            # Producer optimizations
            "enable.idempotence": config.kafka.enable_idempotence,
            "acks": config.kafka.acks,
            "max.in.flight.requests.per.connection": config.kafka.max_in_flight_requests,
            "compression.type": config.kafka.compression_type,
            "batch.size": config.kafka.batch_size,
            "linger.ms": config.kafka.linger_ms,
        }

        # Add SASL auth if configured
        if config.kafka.security_protocol != "PLAINTEXT":
            producer_config.update(
                {
                    "security.protocol": config.kafka.security_protocol,
                    "sasl.mechanism": config.kafka.sasl_mechanism,
                    "sasl.username": config.kafka.sasl_username,
                    "sasl.password": config.kafka.sasl_password,
                }
            )

        self._producer = SerializingProducer(producer_config)
        self._pending_deliveries = 0
        self._failed_deliveries = 0

        self.logger.info(
            "AvroProducer initialized",
            topic=topic,
            schema_type=schema_type,
            bootstrap_servers=config.kafka.bootstrap_servers,
            schema_registry=config.kafka.schema_registry_url,
        )

    def produce(self, event: dict[str, Any]) -> None:
        """Produce event to Kafka topic with Avro serialization.

        Args:
            event: Event dictionary with envelope and payload

        Raises:
            Exception: If produce fails
        """
        try:
            entity_id = event.get("entity_id", "unknown")

            self.logger.debug("Producing event", entity_id=entity_id, topic=self.topic)

            # Produce to Kafka with Avro serialization
            self._producer.produce(
                topic=self.topic,
                key=entity_id,
                value=event,
                on_delivery=self._delivery_callback,
            )

            self._pending_deliveries += 1

            # Poll for delivery reports periodically to prevent buffer overflow
            if self._pending_deliveries % 100 == 0:
                self._producer.poll(0)

        except Exception as e:
            kafka_produce_failed.labels(topic=self.topic).inc()
            self.logger.error(
                "Failed to produce event",
                entity_id=event.get("entity_id"),
                topic=self.topic,
                error=str(e),
                exc_info=True,
            )
            raise

    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages and wait for delivery.

        Args:
            timeout: Timeout in seconds

        Returns:
            Number of messages still pending after timeout

        Raises:
            RuntimeError: If deliveries failed
        """
        initial_pending = self._pending_deliveries
        self.logger.info("Flushing producer", topic=self.topic, pending=initial_pending)

        remaining = self._producer.flush(timeout=timeout)

        if remaining > 0:
            self.logger.warning(
                "Producer flush timeout - messages still pending",
                topic=self.topic,
                remaining=remaining,
            )

        if self._failed_deliveries > 0:
            error_msg = f"Failed to deliver {self._failed_deliveries} messages to {self.topic}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(
            "Producer flushed successfully",
            topic=self.topic,
            delivered=initial_pending - remaining - self._failed_deliveries,
        )

        return remaining

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Kafka delivery callback for async delivery reports.

        Args:
            err: Error if delivery failed
            msg: Message object
        """
        self._pending_deliveries -= 1

        if err:
            self._failed_deliveries += 1
            kafka_produce_failed.labels(topic=self.topic).inc()
            self.logger.error(
                "Message delivery failed",
                topic=msg.topic() if msg else self.topic,
                partition=msg.partition() if msg else None,
                offset=msg.offset() if msg else None,
                error=str(err),
            )
        else:
            kafka_produce_success.labels(topic=self.topic).inc()
            self.logger.debug(
                "Message delivered successfully",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def close(self) -> None:
        """Close the producer and release resources."""
        self.logger.info("Closing producer", topic=self.topic)
        self.flush()
        # SerializingProducer doesn't have explicit close, flush is sufficient

    def __enter__(self) -> "AvroProducer":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures flush on close."""
        self.close()
