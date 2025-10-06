from typing import Any
import mysql.connector
from config import DB_CONFIG

class DatabaseConnection:
    def __init__(self, config=DB_CONFIG):
        self.config = config
        self.conn: Any = None  # <- Any resolve o problema de tipagem

    def __enter__(self):
        self.conn = mysql.connector.connect(**self.config)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
