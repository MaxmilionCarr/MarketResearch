from datahub.hub import MarketHub
from datahub.utils import to_datestr, to_timestamp, date_range_days
import blackscholes
import binomial_tree as bt
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

def _test_binomial(days):
    hub = MarketHub()

    # Pick a ticker and expiry
    ticker = "SPY"
    expiries = hub.option_expiries(ticker)
    if not expiries:
        raise RuntimeError(f"No expiries for {ticker}")
    expiry = expiries[days]

    # Get option chain arrays
    print(hub.op.chain(ticker, expiry))

    K, P, S, r, q, T, cp = hub.option_chain_arrays(ticker, expiry)

    res = pd.DataFrame(index=range(len(K)), columns=["Strike", "Price", "Put/Call", "Historical Vol", "Binomial Price", "Binomial IV"])

    index = 0
    while index < len(P):
        try:
            strike = float(K[index])
            market_px = float(P[index])  # market option price (mid/last)
            time_to_exp = float(T[index])
            rate = 0.04
            div = 0.0
            underlying = float(S[index])
            right = cp[index]  # +1 = Call, -1 = Put

            print(f"Testing {ticker} expiry {expiry}, price {market_px} at strike {strike}")
            print(f"K={strike}, market_px={market_px}, S={underlying}, T={time_to_exp}, r={rate}, q={div}, right={right}")

            # Steps for the binomial tree
            steps = 200
            vol = hub.eq.ewma_volatility(ticker, span=20, period="4mo", interval="1d")
            print(vol)
            vol = vol.iloc[0].item()
            print(f"Using vol: {vol:.4%}")

            if right == 1:
                price = bt.call_price(underlying, strike, time_to_exp, rate, div, vol, steps)
                iv = bt.implied_vol_call(market_px, underlying, strike, time_to_exp, rate, div, steps)
                print(f"Binomial Call price (σ={vol:.4%}): {price:.4f}")
                print(f"Binomial implied vol (from market {market_px:.2f}): {iv:.4%}")
            else:
                price = bt.put_price(underlying, strike, time_to_exp, rate, div, vol, steps)
                iv = bt.implied_vol_put(market_px, underlying, strike, time_to_exp, rate, div, steps)
                print(f"Binomial Put price (σ={vol:.4%}): {price:.4f}")
                print(f"Binomial implied vol (from market {market_px:.2f}): {iv:.4%}")
            res.iloc[index] = [strike, market_px, ("call" if right == 1 else "put"), vol, price, iv]
        except Exception as e:
            res.iloc[index] = [strike, market_px, ("call" if right == 1 else "put"), None, None, None]
            print(f"Error processing index {index}: {e}")
        index += 1
    return res

def _test_blackscholes(days):
    hub = MarketHub()

    # Pick a ticker and expiry
    ticker = "SPY"
    expiries = hub.option_expiries(ticker)
    if not expiries:
        raise RuntimeError(f"No expiries for {ticker}")
    expiry = expiries[days]

    # Get option chain arrays
    print(hub.op.chain(ticker, expiry))
    K, P, S, r, q, T, cp = hub.option_chain_arrays(ticker, expiry)

    index = 0
    while (P[index] <= 0.0):
        index += 1
    index = 9

    strike = float(K[index])
    market_px = float(P[index])  # market option price (mid/last)
    time_to_exp = float(T[index])
    rate = 0.04
    div = 0.0
    underlying = float(S[index])
    right = float(cp[index])  # +1 = Call, -1 = Put

    print(f"Testing {ticker} expiry {expiry}, price {market_px} at strike {strike}")
    print(f"K={strike}, market_px={market_px}, S={underlying}, T={time_to_exp}, r={rate}, q={div}, right={right}")

    # Example: price with a given sigma
    equities = hub.eq
    hist_sigma = equities.ewma_volatility(ticker, span=20, period="4mo", interval="1d")
    sigma_guess = float(hist_sigma.iloc[0])
    print(f"Historical vol: {sigma_guess:.4%}")

    if right == 1:
        price = blackscholes.call_price(underlying, strike, time_to_exp, rate, sigma_guess, div)
        iv = blackscholes.implied_vol_call(market_px, underlying, strike, time_to_exp, rate, div)
        print(f"Call price (σ={sigma_guess}): {price:.4f}")
        print(f"Implied vol (from market {market_px:.2f}): {iv:.4%}")
    else:
        price = blackscholes.put_price(underlying, strike, time_to_exp, rate, sigma_guess, div)
        iv = blackscholes.implied_vol_put(market_px, underlying, strike, time_to_exp, rate, div)
        print(f"Put price (σ={sigma_guess}): {price:.4f}")
        print(f"Implied vol (from market {market_px:.2f}): {iv:.4%}")

'''
hub = MarketHub()              # uses BondsData() inside
curve = hub.bd.curve(refresh=True)
print("r(0.1y)=", curve.r_cc(0.1), " r(2y)=", curve.r_cc(2.0), " r(10y)=", curve.r_cc(10.0))

market = hub.md
print(market.benchmark)

exp = hub.option_expiries("AAPL")[0]

K,P,S,r,q,T,cp = hub.option_chain_arrays("AAPL", exp)
print(hub.op.chain("AAPL", exp))


'''

# _test_blackscholes(1)
options = _test_binomial(3)
print(options[options["Price"] > 0.0])
calls = options[options["Put/Call"] == "call"]
stockprice = yf.Ticker("SPY").history(period="1d")['Close'][0]

plt.plot(calls["Strike"], calls["Binomial IV"], label="Binomial IV", marker='o')
plt.axvline(x=stockprice, color='r', linestyle='--', label='Current Stock Price')
plt.xlabel("Strike Price")
plt.ylabel("Implied Volatility")
plt.title("Binomial Implied Volatility vs Strike Price for SPY Calls")
plt.legend()
plt.grid(True)
plt.show()

