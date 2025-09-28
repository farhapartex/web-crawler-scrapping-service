import sys
import os
import logging
from utils.logger import setup_logging
from tasks.celery_app import celery_app

from config.settings import settings
from events.rabbitmq_listener import start_event_listener_thread

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logging()


def start_celery_worker():
    logger.info("Starting Celery worker")

    start_event_listener_thread()

    celery_app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',
        '--queues=scraping,processing,details'
    ])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "worker":
            start_celery_worker()
        elif command == "start":
            pass
        else:
            print("Usage: python main.py [worker|start]")
            print("  worker - Start Celery worker")
            print("  start  - Start scraping job")