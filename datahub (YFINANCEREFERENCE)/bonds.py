# bonds.py
import numpy as np
import pandas as pd
import yfinance as yf
from typing import Dict, Optional

class YieldCurve:
    def __init__(self, nodes_years, yields_dec):
        order = np.argsort(nodes_years)
        self.T = np.array(nodes_years, dtype=float)[order]
        self.r = np.log1p(np.array(yields_dec, dtype=float)[order])  # approx cc

    def r_cc(self, T: float) -> float:
        T = float(T)
        if T <= self.T[0]: return float(self.r[0])
        if T >= self.T[-1]: return float(self.r[-1])
        return float(np.interp(T, self.T, self.r))

    def r_cc_vec(self, T_arr) -> np.ndarray:
        T_arr = np.asarray(T_arr, float)
        return np.interp(T_arr, self.T, self.r, left=self.r[0], right=self.r[-1])

class BondsData:
    """
    Robust Treasury curve fetcher:
    - Tries multiple Yahoo tickers
    - Falls back to flat curve if only one point (or none, use manual flat)
    """
    def __init__(self, cache: bool = True, manual_flat_rate: Optional[float] = None):
        self.cache = cache
        self._curve: Optional[YieldCurve] = None
        self.manual_flat_rate = manual_flat_rate  # decimal, e.g. 0.04 for 4%

    def _fetch_last_close_pct(self, ticker: str) -> Optional[float]:
        try:
            h = yf.Ticker(ticker).history(period="10d")["Close"].dropna()
            if h.empty: 
                return None
            return float(h.iloc[-1]) / 100.0  # Yahoo yields are in %
        except Exception:
            return None

    def load_nodes(self) -> Dict[float, float]:
        # Candidate Yahoo tickers → maturity (years)
        candidates = {
            "^IRX": 13/52,  # ~0.25y (13-week T-bill)
            "^FVX": 5.0,    # 5Y
            "^TNX": 10.0,   # 10Y
            "^TYX": 30.0,   # 30Y
        }
        nodes: Dict[float, float] = {}
        for tkr, mat in candidates.items():
            y = self._fetch_last_close_pct(tkr)
            if y is not None and np.isfinite(y):
                nodes[mat] = y

        # If nothing came back, fall back to manual flat (if provided)
        if not nodes:
            if self.manual_flat_rate is not None:
                flat = float(self.manual_flat_rate)
                return {1/12: flat, 1.0: flat, 2.0: flat, 5.0: flat, 10.0: flat}
            # Last resort: a reasonable flat default so the pipeline keeps working
            flat = 0.04
            return {1/12: flat, 1.0: flat, 2.0: flat, 5.0: flat, 10.0: flat}

        # If we only have one node, replicate as a flat curve across key tenors
        if len(nodes) == 1:
            only = list(nodes.values())[0]
            return {1/12: only, 0.5: only, 1.0: only, 2.0: only, 5.0: only, 10.0: only}

        return nodes

    def curve(self, refresh: bool = False) -> YieldCurve:
        if self._curve is None or refresh:
            nodes = self.load_nodes()  # always returns something now
            T = list(nodes.keys()); y = list(nodes.values())
            self._curve = YieldCurve(T, y)
        return self._curve
