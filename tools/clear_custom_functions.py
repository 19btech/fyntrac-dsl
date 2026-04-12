#!/usr/bin/env python3
"""Drop the `custom_functions` collection from the configured MongoDB.

Usage: python3 tools/clear_custom_functions.py
This script reads MONGO_URL and DB_NAME from environment or falls back to defaults.
"""
import os
from urllib.parse import urlparse

try:
    from pymongo import MongoClient
except Exception as e:
    raise SystemExit("pymongo is required; run: pip install pymongo")

MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
DB_NAME = os.environ.get('DB_NAME', 'dsl_db')

def main():
    print(f"Connecting to MongoDB at {MONGO_URL}, DB: {DB_NAME}")
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    if 'custom_functions' in db.list_collection_names():
        print("Dropping collection 'custom_functions'...")
        db.custom_functions.drop()
        print("Dropped.")
    else:
        print("No 'custom_functions' collection found; nothing to do.")

if __name__ == '__main__':
    main()
