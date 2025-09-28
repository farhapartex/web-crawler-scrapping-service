from celery import Celery
from config.settings import settings

celery_app = Celery(
    'scraping_service',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=['tasks.scraping_tasks']
)

celery_app.conf.update(
    result_expires=3600,
    timezone='UTC',
    enable_utc=True,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    task_routes={
        'tasks.scraping_tasks.start_scraping_job': {'queue': 'scraping'},
        'tasks.scraping_tasks.process_raw_data': {'queue': 'processing'},
        'tasks.scraping_tasks.process_book_details': {'queue': 'details'},
    }
)