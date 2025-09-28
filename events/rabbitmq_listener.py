import pika
import json
import logging
import threading
from typing import Dict, Set
from datetime import datetime
from config.settings import settings
from database.connection import get_database
from tasks.scraping_tasks import start_scraping_service

logger = logging.getLogger(__name__)

class RabbitMQEventListener:
    def __init__(self):
        self.processed_events: Set[str] = set()
        self.connection = None
        self.channel = None
        self.db = get_database()
        self._load_processed_events()

    def _load_processed_events(self):
        try:
            collection = self.db.processed_events
            events = collection.find({}, {"event_id": 1})
            self.processed_events = {event["event_id"] for event in events}
            logger.info(f"Loaded {len(self.processed_events)} processed events from database")
        except Exception as e:
            logger.error(f"Failed to load processed events: {e}")

    def _mark_event_processed(self, event_id: str):
        try:
            collection = self.db.processed_events
            collection.insert_one({
                "event_id": event_id,
                "processed_at": datetime.utcnow(),
                "status": "completed"
            })
            self.processed_events.add(event_id)
        except Exception as e:
            logger.error(f"Failed to mark event as processed: {e}")

    def _is_event_processed(self, event_id: str) -> bool:
        return event_id in self.processed_events

    def _validate_event(self, event_data: Dict) -> bool:
        required_fields = ["event_id", "event_type", "timestamp"]

        for field in required_fields:
            if field not in event_data:
                logger.error(f"Missing required field: {field}")
                return False

        if event_data["event_type"] != "start_scraping":
            logger.warning(f"Unknown event type: {event_data['event_type']}")
            return False

        return True

    def _process_scraping_event(self, event_data: Dict):
        try:
            event_id = event_data["event_id"]

            if self._is_event_processed(event_id):
                logger.info(f"Event {event_id} already processed, skipping")
                return

            logger.info(f"Processing scraping event {event_id}")

            payload = event_data.get("payload", {})
            url = payload.get("url")

            if url:
                logger.info(f"Starting scraping with custom URL: {url}")
            else:
                logger.info(f"Starting scraping with default URL: {settings.BOOK_SHOP_URL}")

            task_id = start_scraping_service()

            self._mark_event_processed(event_id)

            logger.info(f"Successfully processed event {event_id}, started task {task_id}")

        except Exception as e:
            logger.error(f"Failed to process scraping event: {e}")

    def _callback(self, ch, method, properties, body):
        try:
            event_data = json.loads(body.decode('utf-8'))
            logger.info(f"Received event: {event_data.get('event_id', 'unknown')}")

            if not self._validate_event(event_data):
                logger.error("Invalid event format, rejecting")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if event_data["event_type"] == "start_scraping":
                self._process_scraping_event(event_data)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(settings.RABBITMQ_URL)
            )
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue='scraping_events', durable=True)

            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue='scraping_events',
                on_message_callback=self._callback
            )

            logger.info("Connected to RabbitMQ and ready to consume events")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def start_listening(self):
        try:
            logger.info("Starting RabbitMQ event listener...")
            self.connect()
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping event listener...")
            self.stop_listening()
        except Exception as e:
            logger.error(f"Event listener error: {e}")
            raise

    def stop_listening(self):
        if self.channel:
            self.channel.stop_consuming()
        if self.connection:
            self.connection.close()
        logger.info("RabbitMQ event listener stopped")

def start_event_listener_thread():
    def run_listener():
        listener = RabbitMQEventListener()
        listener.start_listening()

    thread = threading.Thread(target=run_listener, daemon=True)
    thread.start()
    logger.info("Started RabbitMQ event listener in background thread")
    return thread