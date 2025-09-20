from ib_insync import *
from datetime import datetime

# Connect to TWS (7497 = paper, 7496 = live) TEST: 55000 FOR PAPER TRADING
ib = IB()
ib.connect("127.0.0.1", 55000, clientId=1)

# Define SPY contract
spy = Stock("SPY", "SMART", "USD")

# Request 1 year of daily bars
bars = ib.reqHistoricalData(
    spy,
    endDateTime="",            # "" = now
    durationStr="1 D",         # lookback period
    barSizeSetting="5 mins",    # daily bars
    whatToShow="TRADES",       # or MIDPOINT
    useRTH=True,               # only regular trading hours
    formatDate=1
)

# Print summary
print(f"Returned {len(bars)} daily bars")
details = ib.reqContractDetails(spy)
print(f"Trading on {details[0].contract.primaryExchange}")
for b in bars[-5:]:
    print(b.date, b.open, b.high, b.low, b.close, b.volume)

# Dates from IBKR are given as datetime YAY
check = bars[-1].date
print(type(check))
print(isinstance(check, datetime))

ib.disconnect()
