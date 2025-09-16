import sqlite3 as sql
from __future__ import annotations
from typing import Optional, List, Tuple, Any

class HistoricalPricesRepository:
    """
    Data-access layer for the `historical_prices` table.

    Schema:
        ticker_id INTEGER NOT NULL REFERENCES tickers(ticker_id) ON DELETE CASCADE,
        datetime DATETIME NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL NOT NULL,
        volume INTEGER,
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
    # ---------- READ ----------

    def get_all(self) -> List[Tuple[Any, ...]]:
        """
        Return all rows in table
        """
        cur = self.connection.cursor()
        cur.execute("SELECT * FROM historical_prices")
        return cur.fetchall()
    
    def get_info(self, ticker_id: int, start_date: str, end_date: str | None = None) -> List[Tuple[Any, ...]]:
        """
        Return all columns for a given ticker_id and date range.
        If end_date is None, return all data from start_date onwards.
        """
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                "SELECT * FROM historical_prices WHERE ticker_id = ? AND datetime BETWEEN ? AND ? ORDER BY datetime",
                (ticker_id, start_date, end_date)
            )
        else:
            cur.execute(
                "SELECT * FROM historical_prices WHERE ticker_id = ? AND datetime >= ? ORDER BY datetime",
                (ticker_id, start_date)
            )
        return cur.fetchall()

    def get_close_prices(self, ticker_id: int, start_date: str, end_date: str | None = None) -> List[Tuple[str, float]]:
        """
        Return list of (datetime, close) tuples for a given ticker_id and date range.
        If end_date is None, return all data from start_date onwards.
        """
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                "SELECT datetime, close FROM historical_prices WHERE ticker_id = ? AND datetime BETWEEN ? AND ? ORDER BY datetime",
                (ticker_id, start_date, end_date)
            )
        else:
            cur.execute(
                "SELECT datetime, close FROM historical_prices WHERE ticker_id = ? AND datetime >= ? ORDER BY datetime",
                (ticker_id, start_date)
            )
        return cur.fetchall()

    # ---------- CREATE ----------

    def create(self, ticker_id: int, datetime: str, close: float, *, open: float, high: float, low: float, volume: int) -> int:
        """
        Insert a new row and return its primary key.
        Pass column=value pairs as kwargs.
        """

        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO historical_prices (ticker_id, datetime, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ticker_id, datetime, open, high, low, close, volume)
        )
        return cur.lastrowid

    # Wonder how i can implement this for historical prices?
    '''
    def get_or_create(self, ticker_id: int, datetime: str, *, open: float, high: float, low: float, close: float, volume: int) -> int:
        """
        Return the primary key of an existing row with this ticker_id and datetime,
        or create it if it doesn't exist.
        """
        cur = self.connection.cursor()
        cur.execute(
            "SELECT rowid FROM historical_prices WHERE ticker_id = ? AND datetime = ?",
            (ticker_id, datetime)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            return self.create(ticker_id, datetime, close, open=open, high=high, low=low, volume=volume)
    '''

    # ---------- UPDATE ----------

    def update(self, ticker_id: int, datetime: float, *, open: float, high: float, low: float, close: float, volume: int) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """
        cur = self.connection.cursor()
        cur.execute(
            """
            UPDATE historical_prices
            SET open = ?, high = ?, low = ?, close = ?, volume = ?
            WHERE ticker_id = ? AND datetime = ?
            """,
            (open, high, low, close, volume, ticker_id, datetime)
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, ticker_id: int) -> int:
        """
        Delete all rows of price data for a ticker
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute(
            "DELETE FROM historical_prices WHERE ticker_id = ?",
            (ticker_id,)
        )
        self.connection.commit()
        return cur.rowcount
    
    def delete_days(self, ticker_id: int, start_date: str, end_date: str) -> int:
        """
        Delete rows for a given ticker_id and date range.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute(
            "DELETE FROM historical_prices WHERE ticker_id = ? AND datetime BETWEEN ? AND ?",
            (ticker_id, start_date, end_date)
        )
        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL rows from this table.
        Returns number of rows deleted.
        """