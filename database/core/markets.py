from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple

class MarketRepository:
    """
    Data-access layer for the `markets` table.

    Schema:
        market_id INTEGER PRIMARY KEY,
        market_name TEXT NOT NULL
    
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON;")
    
    # ---------- READ ----------

    def get_all(self) -> List[Tuple[int, str, str]]:
        """Return all markets as a list of (id, name, timezone)."""
        cur = self.connection.cursor()
        cur.execute("SELECT market_id, market_name FROM markets")
        return cur.fetchall()

    def get_info(self, market_id: int) -> Optional[Tuple[int, str, str]]:
        """Return a single market row or None if not found."""
        cur = self.connection.cursor()
        try:    
            cur.execute(
                "SELECT market_id, market_name FROM markets WHERE market_id = ?",
                (market_id,),
            )
        except sql.Error as e:
            print(f"SQL error: {e}")
            return None
        return cur.fetchone()

    # ---------- CREATE ----------

    def create(self, market_name: str) -> int:
        """Insert a new market and return its ID."""
        if not market_name:
            raise ValueError("market_name must be provided")
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO markets (market_name) VALUES (?)",
            (market_name,),
        )
        self.connection.commit()
        return cur.lastrowid

    def get_or_create(self, market_name: str) -> int:
        """
        Return the ID of an existing market with this name,
        or create it if it doesn't exist.
        """
        if not market_name:
            raise ValueError("market_name must be provided")
        cur = self.connection.cursor()
        cur.execute(
            "SELECT market_id FROM markets WHERE market_name = ?",
            (market_name,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO markets (market_name) VALUES (?)",
            (market_name,),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------

    def update(
        self,
        market_id: int,
        *,
        market_name: Optional[str] = None
    ) -> int:
        """
        Update name for a market.
        Returns number of rows updated (0 if nothing matched).
        """
        if market_name is None:
            raise ValueError("Must provide at least one field to update")

        fields, values = [], []
        if market_name:
            fields.append("market_name = ?")
            values.append(market_name)

        values.append(market_id)
        sql_query = f"UPDATE markets SET {', '.join(fields)} WHERE market_id = ?"
        cur = self.connection.cursor()
        cur.execute(sql_query, tuple(values))
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(
        self, *, market_id: Optional[int] = None, market_name: Optional[str] = None
    ) -> int:
        """
        Delete a market by id or name.
        Returns number of rows deleted.
        """
        if (market_id is None) == (market_name is None):
            raise ValueError("Provide exactly one of market_id or market_name")

        cur = self.connection.cursor()
        if market_id is not None:
            cur.execute("DELETE FROM markets WHERE market_id = ?", (market_id,))
        else:
            cur.execute("DELETE FROM markets WHERE market_name = ?", (market_name,))

        self.connection.commit()
        return 1

    def delete_all(self) -> int:
        """
        Delete ALL markets.
        Returns number of rows deleted.
        Be sure the caller confirms before calling this.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM markets")
        self.connection.commit()
        return 1