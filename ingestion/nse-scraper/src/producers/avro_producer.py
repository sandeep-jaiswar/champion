"""Avro producer for Kafka."""

import json
from typing import Any, Dict

from src.config import config
from src.utils.logger import get_logger
from src.utils.metrics import kafka_produce_failed, kafka_produce_success

logger = get_logger(__name__)


class AvroProducer:
    """Kafka producer with Avro serialization."""

    def __init__(self, topic: str):
        """Initialize Avro producer.
        
        Args:
            topic: Kafka topic name
        """
        self.topic = topic
        self.logger = get_logger(f"{__name__}.{topic}")
        self._producer = None
        
        # For now, simulate production without actual Kafka
        self.logger.warning("Using mock producer - Kafka integration pending")

    def produce(self, event: Dict[str, Any]) -> None:
        """Produce event to Kafka topic.
        
        Args:
            event: Event dictionary with envelope and payload
        """
        try:
            # Mock production - just log the event
            self.logger.debug("Producing event", 
                            entity_id=event.get("entity_id"),
                            topic=self.topic)
            
            # Simulate Kafka produce
            # In real implementation:
            # self._producer.produce(
            #     topic=self.topic,
            #     key=event["entity_id"],
            #     value=event,
            #     on_delivery=self._delivery_callback
            # )
            
            kafka_produce_success.labels(topic=self.topic).inc()
            
        except Exception as e:
            kafka_produce_failed.labels(topic=self.topic).inc()
            self.logger.error("Failed to produce event", 
                            entity_id=event.get("entity_id"),
                            topic=self.topic,
                            error=str(e))
            raise

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending messages.
        
        Args:
            timeout: Timeout in seconds
        """
        self.logger.info("Flushing producer", topic=self.topic)
        # In real implementation: self._producer.flush(timeout=timeout)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Kafka delivery callback.
        
        Args:
            err: Error if delivery failed
            msg: Message object
        """
        if err:
            self.logger.error("Message delivery failed", error=str(err))
            kafka_produce_failed.labels(topic=self.topic).inc()
        else:
            kafka_produce_success.labels(topic=self.topic).inc()
