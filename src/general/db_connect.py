# src/general/db_connect.py
"""
Single shared DB connection factory. 
Both extract.py and load.py import connect_db() from here.
"""

import os
import psycopg2

# ============================================================================================================


def connect_db():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )