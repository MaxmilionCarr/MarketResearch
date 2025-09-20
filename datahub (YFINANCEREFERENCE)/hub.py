from .equities import EquitiesData
from .options import OptionsData
from .bonds import BondsData
from .market import MarketData

class MarketHub:
    def __init__(self, auto_adjust=True, cache=True, benchmark="SPY"):
        self.eq = EquitiesData(auto_adjust=auto_adjust, cache=cache)
        self.op = OptionsData(cache=cache)
        self.bd = BondsData(cache=cache)
        self.md = MarketData(benchmark=benchmark)

    # convenience pass-throughs
    def adj_close(self, *a, **kw): return self.eq.adj_close(*a, **kw)
    def multi_adj_close(self, *a, **kw): return self.eq.multi_adj_close(*a, **kw)

    def option_expiries(self, symbol): return self.op.expiries(symbol)
    def option_chain(self, symbol, expiry): return self.op.chain(symbol, expiry)

    def option_chain_arrays(self, symbol, expiry, q_default=0.0):
        df = self.op.chain(symbol, expiry)
        curve = self.bd.curve()  # build/interp once; cache inside BondsData
        return self.op.to_arrays(df, rate_fn=curve.r_cc_vec, q=q_default)

