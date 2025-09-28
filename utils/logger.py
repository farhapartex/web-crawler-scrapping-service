import logging
import sys
from config.settings import settings


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('scraping_service.log')
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Logging setup completed")
    return logger