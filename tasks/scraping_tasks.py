import logging
from datetime import datetime
from typing import List
from bson import ObjectId
from celery import current_task
from .celery_app import celery_app
from database.operations import db_ops
from models.schemas import RawData, ProcessedBook, JobStatus
from utils.scraper import scraper
from config.settings import settings

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 3,
        'countdown': 60
    }
)
def start_scraping_job(self, start_url: str):
    try:
        logger.info(f"Starting scraping job for URL: {start_url}")

        current_url = start_url
        total_pages_processed = 0

        while current_url:
            try:
                logger.info(f"Processing page: {current_url}")

                # Create a new sync job for each page/URL
                sync_job_id = db_ops.create_sync_job(current_url)
                db_ops.create_scraping_metrics(sync_job_id)

                soup = scraper.get_page_content(current_url)
                if not soup:
                    logger.warning(f"Failed to get content for {current_url}")
                    db_ops.update_sync_job_status(
                        sync_job_id,
                        JobStatus.FAILED,
                        error_message=f"Failed to get content for {current_url}"
                    )
                    break

                books_data = scraper.extract_books_from_page(soup, current_url, settings.BOOK_SHOP_URL)

                if books_data:
                    raw_data_objects = []
                    for book_data in books_data:
                        raw_data = RawData(
                            sync_job_id=sync_job_id,
                            page_url=book_data['page_url'],
                            book_title=book_data['book_title'],
                            book_url=book_data['book_url'],
                            image_url=book_data['image_url'],
                            price=book_data.get('price', ''),
                            stock_status=book_data.get('stock_status', ''),
                            rating=book_data.get('rating', '')
                        )
                        raw_data_objects.append(raw_data)

                    db_ops.insert_raw_data(raw_data_objects)

                    # Update metrics for this specific sync job
                    db_ops.update_sync_job_metrics(sync_job_id, 1, len(books_data))
                    db_ops.update_scraping_metrics(
                        sync_job_id,
                        total_pages=1,
                        successful_pages=1,
                        total_books_raw=len(books_data)
                    )

                    # Trigger processing for this sync job
                    process_raw_data.delay(str(sync_job_id))

                    logger.info(f"Page {total_pages_processed + 1} completed: {len(books_data)} books found for sync job {sync_job_id}")
                else:
                    logger.warning(f"No books found on page: {current_url}")
                    db_ops.update_sync_job_status(
                        sync_job_id,
                        JobStatus.COMPLETED,
                        completed_at=datetime.utcnow()
                    )

                total_pages_processed += 1

                # Get next URL for the next iteration
                next_url = scraper.get_next_page_url(soup, current_url)
                current_url = next_url

                if current_url:
                    scraper.delay_request()

            except Exception as e:
                logger.error(f"Error processing page {current_url}: {e}")
                if 'sync_job_id' in locals():
                    db_ops.update_sync_job_status(
                        sync_job_id,
                        JobStatus.FAILED,
                        error_message=str(e)
                    )
                break

        logger.info(f"Scraping process completed. Total pages processed: {total_pages_processed}")

        return {
            'total_pages_processed': total_pages_processed,
            'status': 'scraping_completed'
        }

    except Exception as e:
        logger.error(f"Scraping job failed: {e}")
        raise


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 30})
def process_raw_data(self, sync_job_id_str: str):
    try:
        sync_job_id = ObjectId(sync_job_id_str)
        logger.info(f"Processing raw data for sync job: {sync_job_id}")

        unprocessed_data = db_ops.get_unprocessed_raw_data(sync_job_id)

        if not unprocessed_data:
            logger.info("No unprocessed data found")
            db_ops.update_sync_job_status(
                sync_job_id,
                JobStatus.COMPLETED,
                completed_at=datetime.utcnow()
            )
            return {'status': 'no_data_to_process'}

        logger.info(f"Found {len(unprocessed_data)} unprocessed books")

        for raw_data in unprocessed_data:
            process_book_details.delay(str(sync_job_id), str(raw_data['_id']))

        return {
            'sync_job_id': sync_job_id_str,
            'books_to_process': len(unprocessed_data),
            'status': 'processing_started'
        }

    except Exception as e:
        logger.error(f"Failed to process raw data: {e}")
        raise


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 15})
def process_book_details(self, sync_job_id_str: str, raw_data_id_str: str):
    try:
        sync_job_id = ObjectId(sync_job_id_str)
        raw_data_id = ObjectId(raw_data_id_str)

        logger.info(f"Processing book details for raw data: {raw_data_id}")

        raw_data = db_ops.raw_data.find_one({'_id': raw_data_id})
        if not raw_data:
            logger.warning(f"Raw data not found: {raw_data_id}")
            return {'status': 'raw_data_not_found'}

        book_details = scraper.extract_book_details(raw_data['book_url'])

        if book_details:
            processed_book = ProcessedBook(
                sync_job_id=sync_job_id,
                raw_data_id=raw_data_id,
                title=book_details.get('title', raw_data['book_title']),
                image_url=book_details.get('image_url', raw_data['image_url']),
                price_excl_tax=book_details.get('price_excl_tax'),
                price_incl_tax=book_details.get('price_incl_tax'),
                stock_status=book_details.get('stock_status', raw_data['stock_status']),
                star_count=book_details.get('star_count', 0),
                description=book_details.get('description'),
                product_type=book_details.get('product_type'),
                availability=book_details.get('availability'),
                upc=book_details.get('upc'),
                tax=book_details.get('tax'),
                number_of_reviews=book_details.get('number_of_reviews'),
                price_color=book_details.get('price_color')
            )

            db_ops.insert_processed_book(processed_book)
            logger.info(f"Processed book: {processed_book.title}")
        else:
            logger.warning(f"Failed to extract details for book: {raw_data['book_url']}")

        db_ops.mark_raw_data_processed(raw_data_id)

        remaining_count = db_ops.count_unprocessed_raw_data(sync_job_id)
        if remaining_count == 0:
            db_ops.update_sync_job_status(
                sync_job_id,
                JobStatus.COMPLETED,
                completed_at=datetime.utcnow()
            )

            processed_count = db_ops.processed_books.count_documents({"sync_job_id": sync_job_id})
            db_ops.update_scraping_metrics(
                sync_job_id,
                total_books_processed=processed_count,
                end_time=datetime.utcnow()
            )

            logger.info(f"Sync job {sync_job_id} completed successfully")

        scraper.delay_request()

        return {
            'sync_job_id': sync_job_id_str,
            'raw_data_id': raw_data_id_str,
            'status': 'processed',
            'remaining_books': remaining_count
        }

    except Exception as e:
        logger.error(f"Failed to process book details: {e}")
        raise