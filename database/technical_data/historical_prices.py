from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple, Any, Literal
from datetime import datetime

# TODO: Fix up repository with data classes and better methods for fetching

periods = {
        "5 Minutes",
        "1 Hour",
        "1 Day"
    }

# Need to include option to fetch different periods, (5 min, 1 hour, 1 day)
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
    
    def fetch_daily(self, ticker_id: int, start_date: datetime, end_date: datetime | None = None) -> List[Tuple[Any, ...]]:
        """
        Return all columns for a given ticker_id and datetime range with daily period.
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

    def fetch_hourly(self, ticker_id: int, start_date: datetime, end_date: datetime | None = None) -> List[Tuple[Any, ...]]:
        """
        Return all columns for a given ticker_id and datetime range with hourly period.
        If end_date is None, return all data from start_date onwards.
        """
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                """
                SELECT ticker_id, 
                       strftime('%Y-%m-%d %H:00:00', datetime) AS hour,
                       FIRST_VALUE(open) OVER (PARTITION BY strftime('%Y-%m-%d %H', datetime) ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY strftime('%Y-%m-%d %H', datetime) ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM historical_prices
                WHERE ticker_id = ? AND datetime BETWEEN ? AND ?
                GROUP BY hour
                ORDER BY hour
                """,
                (ticker_id, start_date, end_date)
            )
        else:
            cur.execute(
                """
                SELECT ticker_id, 
                       strftime('%Y-%m-%d %H:00:00', datetime) AS hour,
                       FIRST_VALUE(open) OVER (PARTITION BY strftime('%Y-%m-%d %H', datetime) ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY strftime('%Y-%m-%d %H', datetime) ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM historical_prices
                WHERE ticker_id = ? AND datetime >= ?
                GROUP BY hour
                ORDER BY hour
                """,
                (ticker_id, start_date)
            )
        return cur.fetchall()

    def fetch_five_minute(self, ticker_id: int, start_date: datetime, end_date: datetime | None = None) -> List[Tuple[Any, ...]]:
        """
        Return all columns for a given ticker_id and datetime range with 5-minute period.
        If end_date is None, return all data from start_date onwards.
        """
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                """
                SELECT ticker_id, 
                       strftime('%Y-%m-%d %H:%M:00', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') AS five_minute,
                       FIRST_VALUE(open) OVER (PARTITION BY strftime('%Y-%m-%d %H:%M', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY strftime('%Y-%m-%d %H:%M', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM historical_prices
                WHERE ticker_id = ? AND datetime BETWEEN ? AND ?
                GROUP BY five_minute
                ORDER BY five_minute
                """,
                (ticker_id, start_date, end_date)
            )
        else:
            cur.execute(
                """
                SELECT ticker_id, 
                       strftime('%Y-%m-%d %H:%M:00', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') AS five_minute,
                       FIRST_VALUE(open) OVER (PARTITION BY strftime('%Y-%m-%d %H:%M', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY strftime('%Y-%m-%d %H:%M', datetime, '-' || (strftime('%M', datetime) % 5) || ' minutes') ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM historical_prices
                WHERE ticker_id = ? AND datetime >= ?
                GROUP BY five_minute
                ORDER BY five_minute
                """,
                (ticker_id, start_date)
            )
        return cur.fetchall()

    def get_info(self, ticker_id: int, period: Literal["5 Minutes", "1 Hour", "1 Day"], start_date: datetime, end_date: datetime | None = None) -> List[Tuple[Any, ...]]:
        """
        Return all columns for a given ticker_id and datetime range with a specified period.
        If end_date is None, return all data from start_date onwards.
        """
        if period not in periods:
            raise ValueError("Period required")

        match period:
            case "1 Day":
                return self.fetch_daily(ticker_id, start_date, end_date)
            case "1 Hour":
                return self.fetch_hourly(ticker_id, start_date, end_date)
            case "5 Minutes":
                return self.fetch_five_minute(ticker_id, start_date, end_date)
            case _:
                raise ValueError("Invalid period")

    # Use fetch info and cut to get close prices rather than all info
    def get_close_prices(self, ticker_id: int, start_date: datetime, end_date: datetime | None = None) -> List[Tuple[str, float]]:
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

    def create(self, ticker_id: int, datetime: datetime, close: float, *, open: float, high: float, low: float, volume: int) -> int:
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

    def update(self, ticker_id: int, datetime: datetime, *, open: float, high: float, low: float, close: float, volume: int) -> int:
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
    
    def delete_days(self, ticker_id: int, start_date: datetime, end_date: datetime) -> int:
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
        cur = self.connection.cursor()
        cur.execute("DELETE FROM historical_prices")
        self.connection.commit()
        return cur.rowcount