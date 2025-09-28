import sys
import os
import logging
from utils.logger import setup_logging
from tasks.celery_app import celery_app
from tasks.scraping_tasks import start_scraping_job
from config.settings import settings

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logging()


def start_scraping_service():
    logger.info("Starting Book Scraping Service")
    logger.info(f"Base URL: {settings.BOOK_SHOP_URL}")
    logger.info(f"MongoDB: {settings.MONGODB_DB_NAME}")
    logger.info(f"Redis: {settings.REDIS_URL}")

    try:
        result = start_scraping_job.delay(settings.BOOK_SHOP_URL)
        logger.info(f"Scraping job started with task ID: {result.id}")

        print(f"Scraping job initiated successfully!")
        print(f"Task ID: {result.id}")
        print(f"Base URL: {settings.BOOK_SHOP_URL}")
        print(f"Monitor the logs for progress updates.")

        return result.id

    except Exception as e:
        logger.error(f"Failed to start scraping job: {e}")
        print(f"Error starting scraping job: {e}")
        raise


def start_celery_worker():
    logger.info("Starting Celery worker")
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
            start_scraping_service()
        else:
            print("Usage: python main.py [worker|start]")
            print("  worker - Start Celery worker")
            print("  start  - Start scraping job")
    else:
        start_scraping_service()