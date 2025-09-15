import sqlite3 as sql
from __future__ import annotations
from typing import Optional, List, Tuple, Any

class HistoricalPricesRepository:
    """
    Data-access layer for the `historical_prices` table.
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")