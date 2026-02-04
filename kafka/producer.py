"""
Kafka Producer for Real-Time Sales Event Simulation
====================================================

This module implements a Kafka producer that simulates real-time e-commerce
sales events by reading from a sample dataset and publishing events to a 
Kafka topic at configurable intervals.

Production Features:
- Configurable event rate with randomization
- JSON serialization with schema validation
- Error handling with retries
- Graceful shutdown handling
- Comprehensive logging

Author: Data Engineering Team
Version: 1.0.0
"""

import json
import time
import signal
import random
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import csv

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class SalesEvent:
    """
    Data class representing a sales event.
    """
    order_id: str
    customer_id: str
    product_id: str
    product_name: str
    category: str
    price: float
    quantity: int
    order_timestamp: str
    country: str
    
    # Metadata fields added during event creation
    event_time: str = ""
    event_source: str = "kafka_producer"
    
    def __post_init__(self):
        """Add event metadata after initialization."""
        if not self.event_time:
            self.event_time = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    def validate(self) -> bool:
        """
        Validate the sales event.
        """
        validations = [
            bool(self.order_id),
            bool(self.customer_id),
            bool(self.product_id),
            self.price > 0,
            self.quantity > 0,
            bool(self.order_timestamp)
        ]
        return all(validations)


class SalesEventProducer:
    """
    Kafka producer for sales events.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "sales_events",
        data_file: Optional[str] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.data_file = data_file or self._default_data_file()
        self.producer: Optional[KafkaProducer] = None
        self.running = False
        self.events_sent = 0
        self.events_failed = 0
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _default_data_file(self) -> str:
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "sample_data.csv"
        )
    
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def _create_producer(self) -> KafkaProducer:
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10,
                    compression_type='gzip'
                )
                logger.info("Kafka producer created successfully")
                return producer
            except NoBrokersAvailable:
                logger.warning(f"Attempt {attempt + 1}: No brokers available, retrying...")
                time.sleep(retry_delay)
        raise Exception("Could not connect to Kafka after multiple attempts")

    def load_events_from_csv(self) -> list:
        events = []
        try:
            with open(self.data_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    events.append(row)
            return events
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return []

    def start(self, min_interval=1, max_interval=3):
        self.producer = self._create_producer()
        self.running = True
        events = self.load_events_from_csv()
        
        if not events:
            logger.error("No data to produce")
            return

        logger.info("Starting production loop. Press Ctrl+C to stop.")
        while self.running:
            for row in events:
                if not self.running: break
                
                event = SalesEvent(
                    order_id=row['order_id'],
                    customer_id=row['customer_id'],
                    product_id=row['product_id'],
                    product_name=row['product_name'],
                    category=row['category'],
                    price=float(row['price']),
                    quantity=int(row['quantity']),
                    order_timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    country=row['country']
                )
                
                try:
                    self.producer.send(self.topic, key=event.order_id, value=event.to_dict())
                    self.events_sent += 1
                    if self.events_sent % 10 == 0:
                        logger.info(f"Events sent: {self.events_sent}")
                except Exception as e:
                    logger.error(f"Failed to send: {e}")
                    self.events_failed += 1
                
                time.sleep(random.uniform(min_interval, max_interval))

        self.producer.flush()
        self.producer.close()
        logger.info(f"Stopped. Sent: {self.events_sent}, Failed: {self.events_failed}")

if __name__ == "__main__":
    producer = SalesEventProducer()
    producer.start()
