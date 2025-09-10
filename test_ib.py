from ib_insync import *

# Connect to TWS (7497 = paper, 7496 = live)
ib = IB()
ib.connect("127.0.0.1", 4002, clientId=1)

# Define SPY contract
spy = Stock("SPY", "SMART", "USD")

# Request 1 year of daily bars
bars = ib.reqHistoricalData(
    spy,
    endDateTime="",            # "" = now
    durationStr="1 Y",         # lookback period
    barSizeSetting="1 day",    # daily bars
    whatToShow="TRADES",       # or MIDPOINT
    useRTH=True,               # only regular trading hours
    formatDate=1
)

# Print summary
print(f"Returned {len(bars)} daily bars")
for b in bars[:5]:
    print(b.date, b.open, b.high, b.low, b.close, b.volume)

ib.disconnect()
