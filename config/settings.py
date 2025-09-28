import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "book_scraping")

    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

    BOOK_SHOP_URL = os.getenv("BOOK_SHOP_URL", "https://books.toscrape.com")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


settings = Settings()