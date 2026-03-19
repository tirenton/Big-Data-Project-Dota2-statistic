"""
Kafka Producer: Streams Dota 2 game event logs to Kafka topic.
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)


class DotaKafkaProducer:
    """Produces Dota 2 log events to Kafka."""
    
    def __init__(self, broker="kafka:9092", topic="dota2-match-logs", max_retries=30):
        self.broker = broker
        self.topic = topic
        self.producer = None
        self._connect(max_retries)
    
    def _connect(self, max_retries=30):
        """Connect to Kafka with retries."""
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.broker],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_block_ms=10000,
                )
                logger.info(f"Connected to Kafka broker: {self.broker}")
                return
            except NoBrokersAvailable:
                wait_time = min(5 * (attempt + 1), 30)
                logger.warning(
                    f"Kafka not available (attempt {attempt + 1}/{max_retries}). "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
        
        raise ConnectionError(f"Could not connect to Kafka after {max_retries} attempts")
    
    def send_log_event(self, log_event):
        """Send a single log event to Kafka instantly."""
        try:
            match_id = str(log_event.get("match_id", "unknown"))
            
            future = self.producer.send(
                self.topic,
                key=match_id,
                value=log_event
            )
            
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent to {record_metadata.topic}[{record_metadata.partition}] "
                f"offset={record_metadata.offset}: match={match_id} "
                f"hero={log_event.get('hero_name', 'N/A')}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send log event: {e}")
            return False
    
    def send_batch(self, log_events):
        """Send a batch of log events instantly."""
        sent = 0
        failed = 0
        
        for event in log_events:
            if self.send_log_event(event):
                sent += 1
            else:
                failed += 1
        
        self.producer.flush()
        logger.info(f"Batch complete: {sent} sent, {failed} failed")
        return sent, failed
    
    def close(self):
        """Flush and close the producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
