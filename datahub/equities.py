import yfinance as yf
import pandas as pd
import numpy as np

class EquitiesData:
    def __init__(self, auto_adjust=True, cache=True):
        self.auto_adjust = auto_adjust
        self.cache = cache
        self._bars = {}

    def rolling_volatility(self, symbol, window=20, period="6mo", interval="1d", refresh=False):
        """Rolling-window volatility (last available window)"""
        key = (symbol.upper(), period, interval)
        if self.cache and not refresh and key in self._bars:
            return self._bars[key]
        df = yf.download(
            symbol,
            period=period,
            interval=interval,
            auto_adjust=self.auto_adjust,
            progress=False,
            actions=False,
            group_by="column",
        )
        if df.empty:
            raise ValueError(f"No data for {symbol}")
        if self.cache:
            self._bars[key] = df
        returns = np.log(df['Close'] / df['Close'].shift(1)).dropna()
        return returns.rolling(window).std().iloc[-1] * np.sqrt(252)

    def ewma_volatility(self, symbol, span=20, period="6mo", interval="1d", refresh=False):
        """Exponentially weighted moving average volatility"""
        key = (symbol.upper(), period, interval)

        # ✅ if cached and fresh, return cached volatility (not the full DataFrame)
        if self.cache and not refresh and key in self._bars:
            return self._bars[key]["vol"]

        # Download fresh OHLCV data
        df = yf.download(
            symbol,
            period=period,
            interval=interval,
            auto_adjust=self.auto_adjust,
            progress=False,
            actions=False,
            group_by="column",
        )
        if df.empty:
            raise ValueError(f"No data for {symbol}")

        # Compute EWMA volatility
        returns = np.log(df['Close'] / df['Close'].shift(1)).dropna()
        vol = returns.ewm(span=span).std().iloc[-1] * np.sqrt(252)

        # ✅ store both raw data + vol in the cache
        if self.cache:
            self._bars[key] = {"data": df, "vol": vol}

        return vol


    def adj_close(self, symbol, start=None, end=None, interval="1d", refresh=False):
        key = (symbol.upper(), interval, self.auto_adjust)
        
        if self.cache and not refresh and key in self._bars:
            df = self._bars[key]
        else:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=self.auto_adjust,
                progress=False,
                actions=False,
                group_by="column",
            )
            if df.empty:
                raise ValueError(f"No data for {symbol}")
            if self.cache:
                self._bars[key] = df
        s = df['Close'].astype('float32', copy=False)
        if start or end: s = s.loc[start:end]
        s.name = symbol.upper()
        return s

    def multi_adj_close(self, symbols, **kw):
        frames = [self.adj_close(sym, **kw) for sym in symbols]
        return pd.concat(frames, axis=1)