import logging
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from config.settings import settings

logger = logging.getLogger(__name__)


class DatabaseConnection:
    _instance = None
    _client = None
    _database = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def connect(self) -> Database:
        if self._client is None:
            try:
                self._client = MongoClient(settings.MONGODB_URL)
                self._database = self._client[settings.MONGODB_DB_NAME]
                logger.info(f"Connected to MongoDB: {settings.MONGODB_DB_NAME}")
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                raise
        return self._database

    def get_collection(self, collection_name: str) -> Collection:
        if self._database is None:
            self.connect()
        return self._database[collection_name]

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._database = None
            logger.info("MongoDB connection closed")


db_connection = DatabaseConnection()