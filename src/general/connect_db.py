# src/general/db_connect.py
"""
Single shared DB connection factory (each call returns a NEW connection; don't share one across threads).
Both extract.py and load.py import connect_db() from here.
"""

import os
import psycopg2


def connect_db():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )