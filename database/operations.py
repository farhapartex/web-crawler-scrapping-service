import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from bson import ObjectId
from pymongo.collection import Collection
from models.schemas import SyncJob, RawData, ProcessedBook, ScrapingMetrics, JobStatus
from .connection import db_connection

logger = logging.getLogger(__name__)


class DatabaseOperations:
    def __init__(self):
        self.sync_jobs: Collection = db_connection.get_collection("sync_jobs")
        self.raw_data: Collection = db_connection.get_collection("raw_data")
        self.processed_books: Collection = db_connection.get_collection("processed_books")
        self.scraping_metrics: Collection = db_connection.get_collection("scraping_metrics")

    def create_sync_job(self, url: str) -> ObjectId:
        try:
            job = SyncJob(url=url)
            result = self.sync_jobs.insert_one(job.dict(by_alias=True))
            logger.info(f"Created sync job: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Failed to create sync job: {e}")
            raise

    def update_sync_job_status(self, job_id: ObjectId, status: JobStatus,
                             error_message: Optional[str] = None,
                             completed_at: Optional[datetime] = None) -> bool:
        try:
            update_data = {"status": status.value}
            if error_message:
                update_data["error_message"] = error_message
            if completed_at:
                update_data["completed_at"] = completed_at

            result = self.sync_jobs.update_one(
                {"_id": job_id},
                {"$set": update_data}
            )
            logger.info(f"Updated sync job {job_id} status to {status.value}")
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update sync job status: {e}")
            raise

    def update_sync_job_metrics(self, job_id: ObjectId, total_pages: int, total_books: int) -> bool:
        try:
            result = self.sync_jobs.update_one(
                {"_id": job_id},
                {"$set": {
                    "total_pages_scraped": total_pages,
                    "total_books_found": total_books
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update sync job metrics: {e}")
            raise

    def insert_raw_data(self, raw_data_list: List[RawData]) -> List[ObjectId]:
        try:
            documents = [data.dict(by_alias=True) for data in raw_data_list]
            result = self.raw_data.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} raw data records")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Failed to insert raw data: {e}")
            raise

    def get_unprocessed_raw_data(self, sync_job_id: ObjectId) -> List[Dict[str, Any]]:
        try:
            cursor = self.raw_data.find({
                "sync_job_id": sync_job_id,
                "is_data_processed": False
            })
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to get unprocessed raw data: {e}")
            raise

    def mark_raw_data_processed(self, raw_data_id: ObjectId) -> bool:
        try:
            result = self.raw_data.update_one(
                {"_id": raw_data_id},
                {"$set": {
                    "is_data_processed": True,
                    "processed_at": datetime.utcnow()
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to mark raw data as processed: {e}")
            raise

    def insert_processed_book(self, book: ProcessedBook) -> ObjectId:
        try:
            result = self.processed_books.insert_one(book.dict(by_alias=True))
            logger.info(f"Inserted processed book: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Failed to insert processed book: {e}")
            raise

    def get_sync_job(self, job_id: ObjectId) -> Optional[Dict[str, Any]]:
        try:
            return self.sync_jobs.find_one({"_id": job_id})
        except Exception as e:
            logger.error(f"Failed to get sync job: {e}")
            raise

    def count_unprocessed_raw_data(self, sync_job_id: ObjectId) -> int:
        try:
            return self.raw_data.count_documents({
                "sync_job_id": sync_job_id,
                "is_data_processed": False
            })
        except Exception as e:
            logger.error(f"Failed to count unprocessed raw data: {e}")
            raise

    def create_scraping_metrics(self, sync_job_id: ObjectId) -> ObjectId:
        try:
            metrics = ScrapingMetrics(sync_job_id=sync_job_id)
            result = self.scraping_metrics.insert_one(metrics.dict(by_alias=True))
            return result.inserted_id
        except Exception as e:
            logger.error(f"Failed to create scraping metrics: {e}")
            raise

    def update_scraping_metrics(self, sync_job_id: ObjectId, **kwargs) -> bool:
        try:
            result = self.scraping_metrics.update_one(
                {"sync_job_id": sync_job_id},
                {"$set": kwargs}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update scraping metrics: {e}")
            raise


db_ops = DatabaseOperations()