from db import DataBase
from core import exchanges, markets
from instruments import tickers
from technical_data import historical_prices
import sqlite3 as sql
import os
import time

def test_database_create():
    db = DataBase("marketdata.db")
    db.create_db()

def test_database_delete():
    db = DataBase("marketdata.db")
    db.delete_db()
   

def test_exchanges():
    db = DataBase("marketdata.db")
    exch_repo = exchanges.ExchangeRepository(db.connection)
    check = exch_repo.get_or_create("NYSE", timezone="America/New_York")
    info = exch_repo.get_info(check)
    print(info)
    try:
        exch_repo.create("NYSE", "America/New_York")
    except sql.IntegrityError:
        print("Exchange already exists.")
    print(exch_repo.get_all())


if __name__ == "__main__":
    if os.path.exists("marketdata.db"):
        test_database_delete()
    time.sleep(2)
    test_database_create()
    test_exchanges()