from ib_insync import *
import pandas as pd
import json
import sqlite3 as sql
import database as db

DB_PATH = 'marketdata.db'

def save_historical(symbol, exchange, bars, market="equities", currency="USD", description=""):
    con = sql.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    cur = con.cursor()

    ticker_id = db.get_or_create_ticker(cur, symbol, exchange, market, currency, description)

    for bar in bars:
        cur.execute('''INSERT OR REPLACE INTO historical_prices
                       (ticker_id, datetime, open, high, low, close, volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (ticker_id,
                     str(bar.date),   # bar.date is datetime or string
                     bar.open, bar.high, bar.low, bar.close, bar.volume))

    con.commit()
    con.close()
    print(f"✅ Saved {len(bars)} bars for {symbol}")

def fetch_and_save(symbol="SPY", exchange="SMART"):
    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=1)

    contract = Stock(symbol, exchange, "USD")
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr="1 Y",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1
    )

    ib.disconnect()
    save_historical(symbol, exchange, bars, market="equities", currency="USD", description="S&P 500 ETF")

# Run test
if __name__ == "__main__":
    fetch_and_save("SPY", "SMART")
