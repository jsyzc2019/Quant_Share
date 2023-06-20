from .FactorCalc import FactorBase
from ..Utils import lazyproperty, get_tradeDates, get_tradeDate
import numpy as np
import pandas as pd
from datetime import date


DATA_READY = {
    'raw_data': ['chgPct', 'marketValue', 'closePrice', 'turnoverRate', 'bench'],
    'factor': ['LNCAP', 'MIDCAP', 'STOM', 'STOQ', 'STOA', 'Liquidity', 'BETA', 'HSIGMA', 'HALPHA', 'CMRA', 'DASTD', 'ResidualVolatility']
}


class SizeFactor(FactorBase):
    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)

    @lazyproperty
    def LNCAP(self, fill=False) -> pd.DataFrame:
        df = np.log(self.marketValue) if not fill else np.log(self.marketValue).fillna(method='ffill')
        setattr(df, 'name', 'LNCAP')
        return df
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
        setattr(df, 'name', 'MIDCAP')
        return df


class DividendYield(FactorBase):

    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)

    @lazyproperty
    def DTOP(self):
        perCashDiv = self.perCashDiv
        perCashDiv = perCashDiv.resample('D').asfreq().fillna(method='ffill')
        # tds = pd.to_datetime(get_tradeDates(get_tradeDate(self.beginDate, -365*3), self.endDate))
        # tds = perCashDiv.index.intersection(tds)
        # perCashDiv = perCashDiv.loc[tds]
        closePrice = self.closePrice
        perCashDiv, closePrice = self.align_data([perCashDiv, closePrice])
        perCashDiv = sum([perCashDiv.shift(i*63) for i in range(4)])
        df = perCashDiv/closePrice
        df = df.loc[(df.index >= pd.to_datetime(self.beginDate)) & (df.index <= pd.to_datetime(self.endDate))]
        return df

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
        setattr(df, 'name', 'STOM')
        return df

    @lazyproperty
    def STOQ(self, window=63):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,), raw=True)
        setattr(df, 'name', 'STOQ')
        return df

    @lazyproperty
    def STOA(self, window=252):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,), raw=True)
        setattr(df, 'name', 'STOA')
        return df
    @lazyproperty
    def Liquidity(self):
        df = 0.35 * self.STOM + 0.35 * self.STOQ + 0.3 * self.STOA
        setattr(df, 'name', 'Liquidity')
        return


class Volatility(FactorBase):
    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)
        pass

    @lazyproperty
    def BETA(self):
        if 'BETA' in self.__dict__:
            return self.__dict__['BETA']
        beta, alpha, hsigma = self.capm_regress(self.bench, self.chgPct, window=504, half_life=252)
        self.__dict__['HSIGMA'] = hsigma
        self.__dict__['HALPHA'] = alpha
        return beta

    @lazyproperty
    def HSIGMA(self, version: int = None):
        if 'HSIGMA' in self.__dict__:
            return self.__dict__['HSIGMA']
        beta, alpha, hsigma = self.capm_regress(self.bench, self.chgPct, window=504, half_life=252)
        self.__dict__['BETA'] = hsigma
        self.__dict__['HALPHA'] = alpha
        return hsigma

    @lazyproperty
    def HALPHA(self):
        if 'HALPHA' in self.__dict__:
            return self.__dict__['HALPHA']
        beta, alpha, hsigma = self.capm_regress(self.bench, self.chgPct, window=504, half_life=252)
        self.__dict__['BETA'] = beta
        self.__dict__['HSIGMA'] = hsigma
        return alpha

    @lazyproperty
    def CMRA(self, version:int=None):
        log_ret = np.log(1 + self.chgPct)
        cmra = self.rolling_apply(log_ret, self._cal_cmra, args=(12, 21, version), window=252, axis=0).T
        return cmra

    @staticmethod
    def _cal_cmra(series, months=12, days_per_month=21, version=6):
        z = sorted(np.nansum(series[-i * days_per_month:]) for i in range(1, months + 1))
        if version == 6:
            return z[-1] - z[0]
        elif version == 5:
            return np.log(1 + z[-1]) - np.log(1 + z[0])

    @lazyproperty
    def DASTD(self):
        dastd = self.rolling(self.chgPct, window=252,
                             half_life=42, func_name='nanstd')
        return dastd

    @lazyproperty
    def ResidualVolatility(self):
        HSIGMA, CMRA, DASTD = self.align_data([self.HSIGMA, self.CMRA, self.DASTD])
        return 0.1 * HSIGMA + 0.16 * CMRA + 0.74 * DASTD



class BARRA(Liquidity, SizeFactor, Volatility, DividendYield):
    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)
