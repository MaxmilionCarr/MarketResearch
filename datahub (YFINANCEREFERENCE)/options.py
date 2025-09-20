import numpy as np
import pandas as pd
import yfinance as yf

class OptionsData:
    def __init__(self, cache=True):
        self.cache=cache
        self._chains = {}
    
    def expiries(self, symbol):
        return yf.Ticker(symbol).options or []
    
    def chain(self, symbol, expiry, refresh=False):
        key=(symbol.upper(), str(expiry))
        if self.cache and (not refresh) and key in self._chains: return self._chains[key].copy()
        tk = yf.Ticker(symbol)
        oc = tk.option_chain(expiry)
        def norm(df, right):
            bid = pd.to_numeric(df.get("bid"), errors="coerce")
            ask = pd.to_numeric(df.get("ask"), errors="coerce")
            last= pd.to_numeric(df.get("lastPrice", df.get("last")), errors="coerce")
            mid = np.where(bid.notna() & ask.notna(), 0.5*(bid+ask), np.nan)
            out = pd.DataFrame({
                "ticker": symbol.upper(),
                "expiry": pd.to_datetime(expiry, utc=True),
                "right":  right,
                "strike": pd.to_numeric(df.get("strike"), errors="coerce"),
                "bid": bid, "ask": ask, "last": last, "mid": mid,
                "volume": pd.to_numeric(df.get("volume"), errors="coerce"),
                "open_interest": pd.to_numeric(df.get("openInterest"), errors="coerce"),
                "quote_time": pd.Timestamp.now(tz="UTC"),
            })
            return out
        df = pd.concat([norm(oc.calls,"C"), norm(oc.puts,"P")], ignore_index=True)
        # underlying
        try: S = float(tk.fast_info["last_price"])
        except Exception: S = float(tk.history(period="1d")["Close"].iloc[-1])
        df["underlying"] = S
        # working price: require bid/ask to avoid stale last
        have_ba = df["bid"].notna() & df["ask"].notna()
        df["px"] = np.where(have_ba, 0.5*(df["bid"]+df["ask"]), np.nan)
        df = df.dropna(subset=["strike","px"]).sort_values(["right","strike"]).reset_index(drop=True)
        if self.cache: self._chains[key]=df.copy()
        return df

    def to_arrays(self, df, rate_fn=None, q=0.0):
        K = df["strike"].to_numpy("float64")
        P = df["px"].to_numpy("float64")
        S = np.full_like(K, df["underlying"].iloc[0], dtype="float64")
        # time to expiry
        now = pd.Timestamp.now(tz="UTC")
        T  = (pd.to_datetime(df["expiry"], utc=True) - now).dt.total_seconds().to_numpy("float64")/(365.0*24*3600.0)
        T  = np.clip(T, 1e-8, None)

        if rate_fn is not None:
            try:
                r = rate_fn(T)                    # prefer vectorized
            except TypeError:
                # fallback: apply scalar fn over vector
                r = np.array([rate_fn(t) for t in T], dtype=float)
        else:
            r = np.zeros_like(T)
        qv = np.full_like(T, float(q))
        cp = df["right"].map({"C":+1,"P":-1}).to_numpy("int32")
        return K, P, S, r, qv, T, cp