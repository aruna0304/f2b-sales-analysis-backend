import os
from dotenv import load_dotenv

# Load sensitive data from .env file
load_dotenv()

# We assert or default these fields so that if .env is missing, we fail securely or have clear fallbacks
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in the environment or .env file")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise ValueError("DB_NAME is not set in the environment or .env file")