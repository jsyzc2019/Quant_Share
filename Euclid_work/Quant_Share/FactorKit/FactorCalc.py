from ..Utils import lazyproperty, stockList, get_tradeDate
from ..BackTest import DataPrepare, reindex, info_lag, simpleBT, data2score
from ..EuclidGetData import get_data
import pandas as pd
import numpy as np
import statsmodels.api as sm
from functools import reduce
from itertools import chain
from scipy.stats.mstats import winsorize
from datetime import date
import warnings
warnings.filterwarnings('ignore')

from pyfinance.utils import rolling_windows
# from dask import dataframe as dd


import re
import inspect
def varname(p):
    for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
        m = re.search(r'\bvarname\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
        if m:
            return m.group(1)

class FactorBase:
    def __init__(self, beginDate: str = None, endDate: str = None, **kwargs) -> None:
        self.beginDate = beginDate
        self.endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        for k, v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def regress(y, X, intercept: bool = True, weight: int = 1, verbose: bool = True):
        '''
        :param y: 因变量
        :param X: 自变量
        :param intercept: 是否有截距
        :param weight: 权重
        :param verbose: 是否返回残差
        :return:
        '''

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

    def align_data(self, data_lst: list[pd.DataFrame] = [], *args):
        '''
        基于index和columns进行表格的对齐
        :param data_lst: 以列表形式传入需要对齐的表格
        :param args: 可以传入单个表格
        :return:
        '''
        data_lst = list(chain(data_lst, args))
        dims = 1 if any(len(df.shape) == 1 or 1 in df.shape for df in data_lst) else 2
        if len(data_lst) > 2:
            mut_date_range = sorted(reduce(lambda x, y: x.intersection(y), (df.index for df in data_lst)))
            if dims == 2:
                mut_codes = sorted(reduce(lambda x, y: x.intersection(y), (df.columns for df in data_lst)))
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
        '''
        :param series:
        :param n:
        :param mode:
        :param kwargs:
        :return:
        '''
        if mode == 'mad':
            median = np.nanmedian(series)
            mad = np.nanmedian(np.abs(series - median))
            mad_e = 1.4832 * mad
            max_range = median + n * mad_e
            min_range = median - n * mad_e
            return np.clip(series, min_range, max_range)
        elif mode == 'tile':

            return winsorize(np.arange(8), limits=kwargs.get('limits', [0.1, 0.1]), nan_policy='omit')

    @staticmethod
    def nomalize_zscore(series):
        '''
        :param series:
        :return:
        '''
        mu = np.nanmean(series)
        sigma = np.nanstd(series)
        norm = (series - mu) / sigma
        return norm

    @lazyproperty
    def chgPct(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -504),
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'chgPct'])
        df = df.pivot(index='tradeDate', columns='ticker', values='chgPct')
        df.name = 'chgPct'
        return df

    def capm_regress(self, X:pd.DataFrame, Y:pd.DataFrame, window=504, half_life=252):
        X, Y = self.align_data([X, Y])
        beta, alpha, sigma = self.rolling_regress(Y, X, window=window, half_life=half_life)
        return beta, alpha, sigma

    def rolling_regress(self,
                        y,
                        x,
                        window=5,
                        half_life=None,
                        intercept: bool = True,
                        verbose: bool = False,
                        fill_na: str or (int, float) = 0):
        fill_args = {'method': fill_na} if isinstance(fill_na, str) else {'value': fill_na}

        stocks = y.columns
        if half_life:
            weight = self.get_exp_weight(window, half_life)
        else:
            weight = 1

        start_idx = x.loc[pd.notnull(x).values.flatten()].index[0]
        x, y = x.loc[start_idx:], y.loc[start_idx:, :]
        rolling_ys = rolling_windows(y, window)
        rolling_xs = rolling_windows(x, window)

        beta = pd.DataFrame(columns=stocks)
        alpha = pd.DataFrame(columns=stocks)
        sigma = pd.DataFrame(columns=stocks)
        for i, (rolling_x, rolling_y) in enumerate(zip(rolling_xs, rolling_ys)):
            rolling_y = pd.DataFrame(rolling_y,
                                     columns=y.columns,
                                     index=y.index[i:i + window])
            window_sdate, window_edate = rolling_y.index[0], rolling_y.index[-1]
            rolling_y = rolling_y.fillna(**fill_args)
            try:
                rolling_y_val = rolling_y.values
                b, a, resid = self.regress(rolling_y_val, rolling_x,
                                           intercept=True, weight=weight, verbose=True)
            except:
                print(i)
                raise
            vol = np.std(resid, axis=0)

            vol = pd.DataFrame(vol.reshape((1, -1)), columns=stocks, index=[window_edate])
            a = pd.DataFrame(a.reshape((1, -1)), columns=stocks, index=[window_edate])
            b = pd.DataFrame(b, columns=stocks, index=[window_edate])

            beta = pd.concat([beta, b], axis=0)
            alpha = pd.concat([alpha, a], axis=0)
            sigma = pd.concat([sigma, vol], axis=0)

        return beta, alpha, sigma

    @staticmethod
    def get_exp_weight(window: int, half_life: int):
        exp_wt = np.asarray([0.5 ** (1 / half_life)] * window) ** np.arange(window)
        return exp_wt[::-1] / np.sum(exp_wt)

    @lazyproperty
    def marketValue(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=self.beginDate,
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'marketValue'])
        df = df.pivot(index='tradeDate', columns='ticker', values='marketValue')
        df.name = 'marketValue'
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
        df.name = 'closePrice'
        return df

    @lazyproperty
    def turnoverRate(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -252),
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'turnoverRate'])
        df = df.pivot(index='tradeDate', columns='ticker', values='turnoverRate')
        df.name = 'turnoverRate'
        return df

    @lazyproperty
    def bench(self, ticker: str or list[str] = '000300'):
        df = get_data(tableName='gmData_bench_price',
                       begin=get_tradeDate(self.beginDate, -504),
                       end=self.endDate,
                       ticker=ticker,
                       fields=['symbol', 'pre_close', 'adj_factor', 'trade_date'])
        # 指数收益率 =（T指数值 ÷ T-1指数值）× T-1复权因子 - 1
        df['close'] = df['pre_close'].shift(-1)
        df['pre_adj_factor'] = df['adj_factor'].shift(1) if not all(df['adj_factor'] == 0) else 1
        df['return'] = (df['close'] - df['pre_close']) / df['pre_close'] * df['pre_adj_factor']
        df = df[['trade_date', 'return']]
        df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        df.dropna(axis=0, inplace=True)
        df.set_index('trade_date', inplace=True)
        df.name = 'bench'+ticker
        return df[~np.isinf(df)]

    @lazyproperty
    def DataClass(self):
        dc = DataPrepare(beginDate=self.beginDate, endDate=self.endDate)
        dc.get_Tushare_data()
        return dc

    def BackTest(self, data: pd.DataFrame, DataClass=None):
        '''
        :param data:
        :param DataClass:
        :return:
        '''
        if not DataClass: DataClass = self.DataClass
        data_reindex = reindex(data)
        score = info_lag(data2score(data_reindex), n_lag=1)
        # group beck test
        BTClass = simpleBT(DataClass.TICKER, DataClass.BENCH)
        fig, outMetrics, group_res = BTClass.groupBT(score)
        fig.show()  # 绘图
        print(outMetrics)  # 输出指标
