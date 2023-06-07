from .FactorCalc import FactorBase
from ..Utils import lazyproperty
import numpy as np
import pandas as pd
from datetime import date



class SizeFactor(FactorBase):
    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)

    @lazyproperty
    def LNCAP(self, fill=False) -> pd.DataFrame:
        res =  np.log(self.marketValue) if not fill else np.log(self.marketValue).fillna(method='ffill')
        res.name = 'LNCAP'
        return res
    def _calc_MIDCAP(self, x: pd.Series or np.ndarray) -> pd.DataFrame:
        if isinstance(x, pd.Series):
            x = x.values
        y = x ** 3
        beta, alpha, _ = self.regress(y, x, intercept=True, weight=1, verbose=True)
        y_hat = alpha + beta * x
        resid = y - y_hat
        resid = self.winsorize(resid, n=3)
        resid = self.nomalize_zscore(resid)
        return resid

    @lazyproperty
    def MIDCAP(self):
        df = self.LNCAP
        df = df.apply(self._calc_MIDCAP, axis=1, raw=True)
        return df


class DividendYield(FactorBase):

    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)


class Liquidity(FactorBase):

    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)
        pass

    def _calc_Liquidity(self, series, days: int):
        freq = len(series) // days
        res = np.log(np.nansum(series) / freq)
        return -1e10 if np.isinf(res) else res

    @lazyproperty
    def STOM(self, window=21):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,), raw=True)
        return df

    @lazyproperty
    def STOQ(self, window=63):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,), raw=True)
        return df

    @lazyproperty
    def STOA(self, window=252):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,), raw=True)
        return df
    @lazyproperty
    def Liquidity(self):
        return 0.35 * self.STOM + 0.35 * self.STOQ + 0.3 * self.STOA