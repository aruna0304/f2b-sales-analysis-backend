from pymongo import MongoClient
import logging
import certifi
from config import MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

def get_mongo_client():
    """
    Returns a connected MongoClient using credentials from the environment.
    Configured strictly for read-only via standard roles in production,
    but here we connect normally. The application logic guarantees no writes.
    """
    # Using certifi.where() fixes the [SSL: CERTIFICATE_VERIFY_FAILED] error on macOS
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return client

def get_database():

    """
    Returns the target database for forecasting.
    """
    client = get_mongo_client()
    return client[DB_NAME]