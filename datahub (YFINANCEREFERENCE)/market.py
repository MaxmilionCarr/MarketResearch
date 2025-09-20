import numpy as np
import pandas as pd
import yfinance as yf

class MarketData:
    def __init__(self, benchmark="SPY", freq="1d"):
        self.benchmark = benchmark
        self.freq = freq

    def benchmark_returns(self, start=None, end=None, rf_series: pd.Series|None=None) -> pd.DataFrame:
        """Return DataFrame with columns: rm (market simple return), rf (aligned), exm = rm - rf."""
        px = yf.download(self.benchmark, start=start, end=end, interval=self.freq,
                         auto_adjust=True, progress=False)["Close"]
        rm = px.pct_change().dropna()
        rf = rf_series.reindex(rm.index).fillna(method="ffill") if rf_series is not None else pd.Series(0.0, index=rm.index)
        return pd.DataFrame({"rm": rm, "rf": rf, "exm": rm - rf})

    def market_risk_premium(self, start=None, end=None, rf_series: pd.Series|None=None,
                            method="ewm", span=252) -> float:
        """Annualized estimate of E[Rm - Rf]. method: mean | ewm"""
        df = self.benchmark_returns(start, end, rf_series)
        ex = df["exm"]
        if method == "mean":
            mu = ex.mean()
        else:
            mu = ex.ewm(span=span, adjust=False).mean().iloc[-1]
        # annualize (assumes daily freq)
        return float(mu * 252)

    def rolling_beta(self, ri: pd.Series, start=None, end=None, rf_series: pd.Series|None=None,
                     window=252, ewm_span: int|None=None) -> pd.Series:
        """Rolling CAPM beta vs benchmark (simple returns)."""
        df = self.benchmark_returns(start, end, rf_series)
        ri = ri.pct_change().dropna()
        df = df.join(ri.rename("ri"), how="inner")
        if ewm_span:
            # EW beta = Cov_λ(ri,rm) / Var_λ(rm)
            w = df.ewm(span=ewm_span, adjust=False)
            cov = w.cov().unstack().loc[(slice(None),"ri"), ("rm",)]
            var = w.var()["rm"]
            beta = (cov / var).droplevel(0)
        else:
            # Rolling window
            cov = df["ri"].rolling(window).cov(df["rm"])
            var = df["rm"].rolling(window).var()
            beta = cov / var
        return beta.dropna()

    def static_beta(self, ri: pd.Series, start=None, end=None, rf_series: pd.Series|None=None):
        """OLS beta, alpha: ri - rf = alpha + beta*(rm - rf) + eps."""
        df = self.benchmark_returns(start, end, rf_series)
        ri = ri.pct_change().dropna()
        df = df.join(ri.rename("ri"), how="inner").dropna()
        exi = df["ri"] - df["rf"]
        exm = df["rm"] - df["rf"]
        X = np.vstack([np.ones(len(exm)), exm.values]).T
        beta_hat = np.linalg.lstsq(X, exi.values, rcond=None)[0]   # [alpha, beta]
        resid = exi.values - X @ beta_hat
        s2 = (resid @ resid) / (len(exm) - 2)
        var_beta = s2 / (exm.var() * len(exm))
        return {"alpha": beta_hat[0], "beta": beta_hat[1], "beta_se": np.sqrt(var_beta)}