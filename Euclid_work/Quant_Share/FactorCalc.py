
from .Utils import lazyproperty, time_decorator, stockList
from .BackTest import DataPrepare, reindex, info_lag, simpleBT, data2score
from .EuclidGetData import get_data
import pandas as pd
import numpy as np
import statsmodels.api as sm
from functools import reduce
from itertools import chain
from scipy.stats.mstats import winsorize
# from pyfinance.utils import rolling_windows
from datetime import date
# from dask import dataframe as dd
import warnings
warnings.filterwarnings('ignore')

class FactorBase():
    def __init__(self, beginDate:str=None, endDate:str=None, **kwargs) -> None:
        self.beginDate = beginDate
        self.endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        for k,v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def regress(y, X, intercept: bool = True, weight: int = 1, verbose: bool = True):

        if len(X.shape) == 1: X = X.reshape((-1, 1))
        if intercept:
            const = np.ones(len(X))
            X = np.insert(X, 0, const, axis=1)

        model = sm.WLS(y, X, weights=weight, missing='drop')
        result = model.fit()
        params = result.params
        if verbose:
            resid = y - np.dot(X, params)
            if intercept:
                return params[1:], params[0], resid
            else:
                return params, None, resid
        else:
            if intercept:
                return params[1:], params[0]
            else:
                return params

    def align_data(self, data_lst:list[pd.DataFrame]=[], *args):
        # data_lst = [df.set_index(on) for df in data_lst if on in df.columns]
        data_lst = list(chain(data_lst, args))
        dims = 1 if any(len(df.shape) == 1 or 1 in df.shape for df in data_lst) else 2
        if len(data_lst) > 2:
            mut_date_range = sorted(reduce(lambda x,y: x.intersection(y), (df.index for df in data_lst)))
            if dims == 2:
                mut_codes = sorted(reduce(lambda x,y: x.intersection(y), (df.columns for df in data_lst)))
                data_lst = [df.loc[mut_date_range, mut_codes] for df in data_lst]
            elif dims == 1:
                data_lst = [df.loc[mut_date_range, :] for df in data_lst]
        else:
            mut_date_range = sorted(data_lst[0].index.intersection(data_lst[1].index))
            if dims == 2:
                mut_codes = sorted(data_lst[0].columns.intersection(data_lst[1].columns))
                data_lst = [df.loc[mut_date_range, mut_codes] for df in data_lst]
            elif dims == 1:
                data_lst = [df.loc[mut_date_range, :] for df in data_lst]
        return data_lst

    @staticmethod
    def winsorize(
            series: pd.Series or np.ndarray,
            n: float or int = 3,
            mode: str = 'mad',
            **kwargs
    ):
        if mode == 'mad':
            median = np.nanmedian(series)
            mad = np.nanmedian(np.abs(series - median))
            mad_e = 1.4832 * mad
            max_range = median + n * mad_e
            min_range = median - n * mad_e
            return np.clip(series, min_range, max_range)
        elif mode == 'tile':
            # not done
            return winsorize(np.arange(8), limits=kwargs.get('limits', [0.1, 0.1]), nan_policy='omit')

    @staticmethod
    def nomalize_zscore(series):
        mu = np.nanmean(series)
        sigma = np.nanstd(series)
        norm = (series - mu) / sigma
        return norm

    @lazyproperty
    def marketValue(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=self.beginDate,
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'marketValue'])
        df = df.pivot(index='tradeDate', columns='ticker', values='marketValue')
        return df

    @lazyproperty
    def closePrice(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=self.beginDate,
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'closePrice'])
        df = df.pivot(index='tradeDate', columns='ticker', values='closePrice')
        return df

    @lazyproperty
    def turnoverRate(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=self.beginDate,
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'turnoverRate'])
        df = df.pivot(index='tradeDate', columns='ticker', values='turnoverRate')
        return df

    @lazyproperty
    def DataClass(self):
        dc = DataPrepare(beginDate=self.beginDate, endDate=self.endDate)
        dc.get_Tushare_data()
        return dc

    def BackTest(self, data:pd.DataFrame, DataClass=None):
        if not DataClass: DataClass = self.DataClass
        data_reindex = reindex(data)
        score = info_lag(data2score(data_reindex), n_lag=1)
        # group beck test
        BTClass = simpleBT(DataClass.TICKER, DataClass.BENCH)
        fig, outMetrics, group_res = BTClass.groupBT(score)
        fig.show() # 绘图
        print(outMetrics) # 输出指标

class SizeFactor(FactorBase):
    def __init__(self, beginDate:str=None, endDate:str=None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)

    @lazyproperty
    def LNCAP(self, fill=False) -> pd.DataFrame:
        return np.log(self.marketValue) if not fill else np.log(self.marketValue).fillna(method='ffill')

    def _calc_MIDCAP(self, x:pd.Series or np.ndarray) -> pd.DataFrame:
        if isinstance(x, pd.Series):
            x = x.values
        y = x**3
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

    def __init__(self, beginDate:str=None, endDate:str=None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)
        pass
    def _calc_Liquidity(self, series, days:int):
        freq = len(series) // days
        res = np.log(np.nansum(series)/freq)
        return -1e10 if np.isinf(res) else res
    @lazyproperty
    def STOM(self, window=21):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,),raw=True)
        return df
    @lazyproperty
    def STOQ(self, window=63):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,),raw=True)
        return df
    @lazyproperty
    def STOA(self, window=252):
        df = self.turnoverRate
        df = df.rolling(window=window, axis=0).apply(self._calc_Liquidity, args=(21,),raw=True)
        return df

    @lazyproperty
    def Liquidity(self):
        return 0.35 * self.STOM + 0.35 * self.STOQ + 0.3 *self.STOA



