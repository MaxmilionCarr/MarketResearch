import datetime
from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple
   


# !! TO DO !!
class TickerRepository:
    """
    Data-access layer for the `tickers` table.
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")

class EquitiesRepository:
    """
    Data-access layer for equities-related tables.
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")

class HistoricalDataRepository:
    """
    Data-access layer for historical data tables.
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")

class OptionsRepository:
    """
    Data-access layer for options-related tables.
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")