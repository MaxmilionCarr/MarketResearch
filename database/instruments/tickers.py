from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple, Any

# This is the primary table for all instruments
class TickerRepository:
    """
    Data-access layer for the `tickers` table.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        market_id INTEGER NOT NULL,
        exchange_id INTEGER NOT NULL,
        currency TEXT NOT NULL,
        description TEXT,
        source TEXT NOT NULL,
        UNIQUE(symbol, exchange_id),
        FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
    # ---------- READ ----------

    def get_all(self):
        """Return all tickers as a list of tuples."""
        cur = self.connection.cursor()
        cur.execute("SELECT ticker_id, symbol, market_id, exchange_id, currency, description, source FROM tickers")
        return cur.fetchall()

    def get_info(self, *, symbol: str | None = None, ticker_id: int | None = None) -> Optional[Tuple[Any, ...]]:
        """
        Return a single row by primary key or None if not found.
        """
        if symbol is None and ticker_id is None:
            raise ValueError("Must provide symbol or ticker_id")
        cur = self.connection.cursor()
        if ticker_id is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, description, source FROM tickers WHERE ticker_id = ?",
                    (ticker_id,),
                )
                return cur.fetchone()
            except sql.Error as e:
                print(f"Error fetching ticker by ticker_id: {e}")
                pass
            
        if symbol is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, description, source FROM tickers WHERE symbol = ?",
                    (symbol,),
                )
                return cur.fetchone()
            except sql.Error as e:
                print(f"Error fetching ticker by symbol: {e}")
                pass

        return -1
    # ---------- CREATE ----------

    def create(self, symbol: str, market_id: int, exchange_id: int, currency: str, description: str, source: str) -> int:
        """
        Insert a new ticker and return its ID.
        """
        cur = self.connection.cursor()
        cur.execute(
            f"INSERT INTO tickers (symbol, market_id, exchange_id, currency, description, source) VALUES (?, ?, ?, ?, ?, ?)",
            (symbol, market_id, exchange_id, currency, description, source),
        )
        self.connection.commit()
        return cur.lastrowid  


    def get_or_create(self, symbol: str, market_id: int, exchange_id: int, currency: str, source: str, *, description: str | None = None) -> int:
        """
        Return the ID of a row where unique_col == unique_val,
        or create it using defaults if it doesn't exist.
        """
        cur = self.connection.cursor()
        cur.execute(
            f"SELECT ticker_id FROM tickers WHERE symbol = ? AND market_id = ? AND exchange_id = ? AND currency = ?",
            (symbol, market_id, exchange_id, currency),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            f"INSERT INTO tickers (symbol, market_id, exchange_id, currency, description, source) VALUES (?, ?, ?, ?, ?, ?)",
            (symbol, market_id, exchange_id, currency, description or "", source),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------

    def update(self, ticker_id: str, *, symbol: str, market_id: int, exchange_id: int, currency: str, description: str | None = None, source: str | None = None) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """
        if not (symbol or market_id or exchange_id or currency or description or source):
            raise ValueError("Must provide at least one field to update")
        cur = self.connection.cursor()
        fields = []
        values = []
        if symbol is not None:
            fields.append("symbol = ?")
            values.append(symbol)
        if market_id is not None:
            fields.append("market_id = ?")
            values.append(market_id)
        if exchange_id is not None:
            fields.append("exchange_id = ?")
            values.append(exchange_id)
        if currency is not None:
            fields.append("currency = ?")
            values.append(currency)
        if description is not None:
            fields.append("description = ?")
            values.append(description)
        if source is not None:
            fields.append("source = ?")
            values.append(source)
        values.append(ticker_id)
        cur.execute(
            f"UPDATE tickers SET {', '.join(fields)} WHERE ticker_id = ?",
            tuple(values),
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker_id: int | None = None, symbol: str | None = None) -> int:
        """
        Delete a row by primary key or symbol.
        Returns number of rows deleted.
        """
        if ticker_id is None and symbol is None:
            raise ValueError("Must provide ticker_id or symbol")
        cur = self.connection.cursor()
        if ticker_id is not None:
            cur.execute("DELETE FROM tickers WHERE ticker_id = ?", (ticker_id,))
        elif symbol is not None:
            cur.execute("DELETE FROM tickers WHERE symbol = ?", (symbol,))
        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL tickers.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM tickers")
        self.connection.commit()
        return cur.rowcount


# These tables provide definitions on the data availability for 
# Different market types
class EquitiesRepository:
    """
    Data-access layer for equities-related tables.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        sector TEXT,
        industry TEXT,
        dividend_yield REAL,
        pe_ratio REAL,
        eps REAL,
        beta REAL,
        market_cap REAL,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
    # ---------- READ ----------

    def get_all(self) -> List[Tuple[Any, ...]]:
        """
        Return all rows as a list of tuples of all info in equities.
        """
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities"
        )
        return cur.fetchall()

    def get_info(self, *, ticker_id: int, symbol: str) -> Optional[Tuple[Any, ...]]:
        """
        Return a single row by primary key or None if not found.
        """
        cur = self.connection.cursor()
        if ticker_id is not None:
            try:
                cur.execute(
                    "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities WHERE ticker_id = ?",
                    (ticker_id,),
                )
                return cur.fetchone()
            except sql.Error as e:
                print(f"Error fetching equity by ticker_id: {e}")
                pass
        if symbol is not None:
            try:
                cur.execute(
                    "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities WHERE symbol = ?",
                    (symbol,),
                )
                return cur.fetchone()
            except sql.Error as e:
                print(f"Error fetching equity by symbol: {e}")
                pass

        return -1

    # ---------- CREATE ----------

    def create(self, ticker_id: int, symbol: str, sector: str, industry: str, dividend_yield: float, pe_ratio: float, eps: float, beta: float, market_cap: float) -> int:
        """
        Insert a new row and return its primary key.
        """
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equities (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap),
        )
        self.connection.commit()
        return cur.lastrowid

    def get_or_create(self, *, ticker_id: int, symbol: str, sector: str, industry: str, dividend_yield: float, pe_ratio: float, eps: float, beta: float, market_cap: float) -> int:
        """
        Return the ID of a row where unique_col == unique_val,
        or create it using defaults if it doesn't exist.
        """
        cur = self.connection.cursor()
        if (ticker_id is None and symbol is None):
            raise ValueError("Must provide ticker_id or symbol")
        
        if ticker_id is not None:
            info = self.get_info(ticker_id=ticker_id)
            if info != -1:
                return info[0]
        if symbol is not None:
            info = self.get_info(symbol=symbol)
            if info != -1:
                return info[0]
            
        if ticker_id is not None and symbol is not None:
            cur.execute(
                "INSERT INTO equities (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap),
            )
            self.connection.commit()
            return cur.lastrowid
        
        raise ValueError("Must provide ticker_id and symbol to create a new equity")
    
    # ---------- UPDATE ----------

    def update(self, *, ticker_id: int, symbol: str, sector: str, industry: str, dividend_yield: float, pe_ratio: float, eps: float, beta: float, market_cap: float) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """
        if ticker_id is None and symbol is None:
            raise ValueError("Must provide ticker_id or symbol")
        cur = self.connection.cursor()
        fields = []
        values = [] 
        if ticker_id is not None:
            fields.append("ticker_id = ?")
            values.append(ticker_id)
        if symbol is not None:
            fields.append("symbol = ?")
            values.append(symbol)
        if sector is not None:
            fields.append("sector = ?")
            values.append(sector)
        if industry is not None:
            fields.append("industry = ?")
            values.append(industry)
        if dividend_yield is not None:
            fields.append("dividend_yield = ?")
            values.append(dividend_yield)
        if pe_ratio is not None:
            fields.append("pe_ratio = ?")
            values.append(pe_ratio)
        if eps is not None:
            fields.append("eps = ?")
            values.append(eps)
        if beta is not None:
            fields.append("beta = ?")
            values.append(beta)
        if market_cap is not None:
            fields.append("market_cap = ?")
            values.append(market_cap)
            
        if ticker_id is not None:
            cur.execute(
                f"UPDATE equities SET {', '.join(fields)} WHERE ticker_id = ?",
                tuple(values) + (ticker_id,),
            )
        elif symbol is not None:
            cur.execute(
                f"UPDATE equities SET {', '.join(fields)} WHERE symbol = ?",
                tuple(values) + (symbol,),
            )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker_id: int = None, symbol: str = None) -> int:
        """
        Delete a row by primary key OR by custom filters.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        if ticker_id is not None:
            cur.execute("DELETE FROM equities WHERE ticker_id = ?", (ticker_id,))
        elif symbol is not None:
            cur.execute("DELETE FROM equities WHERE symbol = ?", (symbol,))
        else:
            raise ValueError("Must provide ticker_id or symbol")

        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL rows from this table.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equities")
        self.connection.commit()
        return cur.rowcount

# Not needed atm, first test implementation of equities
class BondsRepository:
    """
    Data-access layer for bonds-related tables.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        bond_type TEXT,
        maturity_date TEXT,
        coupon REAL,
        yield REAL,
        credit_rating TEXT,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
