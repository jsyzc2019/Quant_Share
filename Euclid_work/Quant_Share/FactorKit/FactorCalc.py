from ..Utils import lazyproperty, stockList, get_tradeDate, time_decorator, get_tradeDates, tradeDateList
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

from multiprocessing import cpu_count
from pyfinance.utils import rolling_windows
from dask import dataframe as dd
import re
import inspect
from functools import cached_property

def varname(p):
    for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
        m = re.search(r'\bvarname\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
        if m:
            return m.group(1)

class FactorData():
    def __init__(self, beginDate: str = None, endDate: str = None, **kwargs) -> None:
        self.beginDate = beginDate
        self.endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        for k, v in kwargs.items():
            setattr(self, k, v)

    @cached_property
    def industry(self):
        df = get_data('IndustryID_Sw21',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=['date', 'winCode', 'industryName1'])
        df['date'] = pd.to_datetime(df['date'])
        df = reindex(df.pivot(index='date', values='industryName1', columns='winCode'))
        return df

    @cached_property
    def net_cf_inv(self):
        """
        投资活动现金流出
        :return:
        """

        df = get_data(tableName='fundamentals_cashflow',
                      begin=get_tradeDate(self.beginDate, -365*2),
                      end=self.endDate,
                      fields=['symbol', 'net_cf_inv', 'pub_date'])
        df['pub_date'] = pd.to_datetime(df['pub_date'])
        df = df.sort_values(by='pub_date')
        df = df.drop_duplicates(subset=['symbol', 'pub_date'], keep='last')
        df = df.pivot(values='net_cf_inv', index='pub_date', columns='symbol')
        return df

    @cached_property
    def net_cf_oper(self):
        """
        经营活动产生的现金流量净值
        :return:
        """

        df = get_data(tableName='fundamentals_cashflow',
                      begin=get_tradeDate(self.beginDate, -365*2),
                      end=self.endDate,
                      fields=['symbol', 'net_cf_oper', 'pub_date'])
        df['pub_date'] = pd.to_datetime(df['pub_date'])
        df = df.sort_values(by='pub_date')
        df = df.drop_duplicates(subset=['symbol', 'pub_date'], keep='last')
        df = df.pivot(values='net_cf_oper', index='pub_date', columns='symbol')
        return df

    @cached_property
    def SP(self):
        df = 1/self.PS
        return df

    @cached_property
    def PS(self):
        """
        市销率TTM = 总市值 / 营业收入TTM
        :return:
        """
        df = get_data(tableName='MktEqudEval',
                      begin=self.beginDate, end=self.endDate,
                      fields=['ticker', 'PS', 'tradeDate'])
        df = df.pivot(values='PS', index='tradeDate', columns='ticker')
        return df


    @cached_property
    def BP(self):
        """
        净资产/总市值
        :return:
        """
        PB = self.PB
        return 1/PB


    @cached_property
    def PB(self):
        df = get_data(tableName='MktEqud',
                      begin=self.beginDate, end=self.endDate,
                      fields=['ticker', 'PB', 'tradeDate'])
        df = df.pivot(values='PB', index='tradeDate', columns='ticker')
        return df

    @cached_property
    def EPTTM(self):
        df = self.PETTM
        df[df == 0] == 1/np.Inf
        df = 1/df
        return df

    @cached_property
    def PETTM(self):
        df = get_data(tableName='trading_derivative_indicator',
                      begin=self.beginDate, end=self.endDate,
                      fields=['symbol', 'PETTM', 'pub_date'])
        df['pub_date'] = pd.to_datetime(df['pub_date'])
        df = df.sort_values(by='pub_date')
        df = df.drop_duplicates(subset=['symbol', 'pub_date'], keep='last')
        df = df.pivot(values='PETTM', index='pub_date', columns='symbol')
        return df

    @cached_property
    def net_prof(self):
        """
        净利润
        :return:
        """
        df = get_data(tableName='fundamentals_cashflow',
                         begin=get_tradeDate(self.beginDate, -2 * 365),
                         end=self.endDate,
                         fields=['symbol', 'net_prof', 'pub_date'])
        df['pub_date'] = pd.to_datetime(df['pub_date'])
        df = df.sort_values(by='pub_date')
        df = df.drop_duplicates(subset=['symbol', 'pub_date'], keep='last')
        df = df.pivot(values='net_prof', index='pub_date', columns='symbol')
        return df

    @cached_property
    def NPCUT(self):
        """
        扣除非经常性损益的净利润
        :return:
        """
        NPCUT = get_data(tableName='deriv_finance_indicator',
                        begin=get_tradeDate(self.beginDate, -2*365),
                        end=self.endDate,
                        fields=['symbol', 'NPCUT', 'pub_date'])
        NPCUT['pub_date'] = pd.to_datetime(NPCUT['pub_date'])
        NPCUT = NPCUT.sort_values(by='pub_date')
        NPCUT = NPCUT.drop_duplicates(subset=['symbol', 'pub_date'], keep='last')
        NPCUT = NPCUT.pivot(values='NPCUT', index='pub_date', columns='symbol')
        return NPCUT

    @cached_property
    def perCashDiv(self):
        df = get_data(tableName='EquDiv_info', verbose=False)
        df = df.sort_values(by='publishDate')
        df = df.dropna(subset='publishDate')
        df = df.drop_duplicates(subset=['ticker', 'publishDate'], keep='last')
        df = df.pivot(values='perCashDiv', index='publishDate', columns='ticker')
        df.index = pd.to_datetime(df.index)
        # df = df.resample('D').asfreq().fillna(method='ffill')
        # tds = pd.to_datetime(get_tradeDates(get_tradeDate(self.beginDate, -365*3), self.endDate))
        # tds = df.index.intersection(tds)
        # df = df.loc[tds]
        df.name = 'perCashDiv'
        return df

    @lazyproperty
    def EPS(self):
        df = get_data(
            tableName='FdmtIndiPSPit',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -365*3),
            end=self.endDate)
        df['publishDate'] = pd.to_datetime(df['publishDate'])
        df = df.sort_values(by='publishDate')
        df = df.drop_duplicates(subset=['ticker', 'publishDate'], keep='last')
        df = df.pivot(index='publishDate', columns='ticker', values='EPS')
        # df = df.resample('D').asfreq().fillna(method='ffill')
        # tds = pd.to_datetime(get_tradeDates(get_tradeDate(self.beginDate, -365*3), self.endDate))
        # tds = df.index.intersection(tds)
        # df = df.loc[tds]
        df.name = 'EPS'
        return df

    @lazyproperty
    def turnoverValue(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -504),
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'turnoverValue'])
        df = df.pivot(index='tradeDate', columns='ticker', values='turnoverValue')
        df.name = 'turnoverValue'
        return df

    @lazyproperty
    def negMarketValue(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -504),
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'negMarketValue'])
        df = df.pivot(index='tradeDate', columns='ticker', values='negMarketValue')
        df.name = 'negMarketValue'
        return df

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

    @lazyproperty
    def marketValue(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=self.beginDate,
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'marketValue'])
        df = df.pivot(index='tradeDate', columns='ticker', values='marketValue')
        setattr(df, 'name', 'marketValue')
        return df

    @lazyproperty
    def closePrice(self):
        df = get_data(
            tableName='MktEqud',
            ticker=stockList,
            begin=get_tradeDate(self.beginDate, -365),
            end=self.endDate,
            fields=['ticker', 'tradeDate', 'closePrice'])
        df = df.pivot(index='tradeDate', columns='ticker', values='closePrice')
        setattr(df, 'name', 'closePrice')
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
        setattr(df, 'name', 'turnoverRate')
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
        # df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        df.dropna(axis=0, inplace=True)
        df.set_index('trade_date', inplace=True)
        setattr(df, 'name', 'bench' + ticker)
        return df[~np.isinf(df)]

class FactorBase(FactorData):
    def __init__(self, beginDate: str = None, endDate: str = None, **kwargs) -> None:
        self.beginDate = beginDate
        self.endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        for k, v in kwargs.items():
            setattr(self, k, v)

    def adjust(self, df:pd.DataFrame, method:str = None):
        res = pd.DataFrame(data=0, columns=df.columns, index=pd.date_range(self.beginDate, self.endDate))
        res = res.loc[res.index.intersection(pd.to_datetime(tradeDateList))]
        inter_index = df.index.intersection(res.index)
        res.loc[inter_index] = df.loc[inter_index]
        if method:
            res = res.fillna(method=method)
        return res

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
        # if len(y.shape) == 1: y = y.reshape((-1, 1))
        if intercept:
            const = np.ones(len(X))
            X = np.insert(X, 0, const, axis=1)
        try:
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
        except ValueError as e:
            print(e)
            print(X.shape, y.shape, X.size, y.size)

    def align_data(self, data_lst: list[pd.DataFrame] = [], clean:bool = True, *args):
        '''
        基于index和columns进行表格的对齐
        :param data_lst: 以列表形式传入需要对齐的表格
        :param args: 可以传入单个表格
        :return:
        '''
        data_lst = list(chain(data_lst, args))
        if clean:
            data_lst = [reindex(df) for df in data_lst]
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
        if isinstance(series, (list, pd.Series, np.ndarray)):
            mu = np.nanmean(series)
            sigma = np.nanstd(series)
            norm = (series - mu) / sigma
            return norm
        elif isinstance(series, pd.DataFrame):
            mu = np.nanmean(series, axis=1).reshape((-1,1))
            sigma = np.nanstd(series, axis=1).reshape((-1,1))
            norm = (series.values - mu) / sigma
            return pd.DataFrame(norm, index=series.index, columns=series.columns)
        else:
            raise NotImplementedError




    def capm_regress(self, X:pd.DataFrame, Y:pd.DataFrame, window=504, half_life=252):
        X, Y = self.align_data([X, Y])
        beta, alpha, sigma = self.rolling_regress(Y, X, window=window, half_life=half_life)
        return beta, alpha, sigma

    def rolling(self, datdf, window, half_life=None,
                func_name='nansum', weights=None):

        func = getattr(np, func_name)
        if func is None:
            msg = f"""Search func:{func_name} from numpy failed,
                   only numpy ufunc is supported currently, please retry."""
            raise AttributeError(msg)

        if half_life or (weights is not None):
            exp_wt = self.get_exp_weight(window, half_life) if half_life else weights
            args = func, exp_wt
        else:
            args = func

        try:
            res = self.pandas_parallelcal(datdf, self.nanfunc, args=args,
                                          window=window)
        except Exception:
            print('Calculating under single core mode...')
            res = self.rolling_apply(datdf, self.nanfunc, args=args,
                                     axis=0, window=window)
        return res

    def rolling_apply(self, datdf, func, args=None, axis=0, window=None):
        if window:
            res = datdf.rolling(window=window).apply(func, args=args, raw=True)
        else:
            res = datdf.apply(func, args=args, axis=axis, raw=True)
        return res

    @staticmethod
    def round_up_to_hundred(num):
        return int(np.ceil(num / 100) * 100)

    @staticmethod
    @time_decorator
    def pandas_parallelcal(dat, myfunc, args=None, window=None):
        if window:
            ncores = len(dat)//window
        else:
            ncores = 6
        print(f'Try with npartitions={ncores}')
        res = dd.from_pandas(dat, npartitions=ncores)
        if window:
            res = res.rolling(window=window, axis=0)
            if args is None:
                res = res.apply(myfunc, raw=True)
            else:
                res = res.apply(myfunc, args=args, raw=True)
        else:
            res = res.apply(myfunc, args=args, axis=1)
        # distributed, multiprocessing, processes, single-threaded, sync, synchronous, threading, threads
        return res.compute(scheduler='processes')

    def nanfunc(self, series, func, weights=None):
        if weights is not None:
            return self.weighted_func(func, series, weights=weights)
        else:
            return func(series)

    def weighted_func(self, func, series, weights):
        weights /= np.nansum(weights)
        if 'std' in func.__name__:
            return self.weighted_std(series, weights)
        else:
            return func(series * weights)

    @staticmethod
    def weighted_std(series, weights):
        return np.sqrt(np.nansum((series - np.nanmean(series)) ** 2 * weights))

    def rolling_regress(self,
                        y,
                        x,
                        window=5,
                        half_life=None,
                        fill_na: str or (int, float) = 0):

        fill_args = {'method': fill_na} if isinstance(fill_na, str) else {'value': fill_na}

        stocks = y.columns
        if half_life:
            weight = self.get_exp_weight(window, half_life)
        else:
            weight = 1

        start_idx = x.index[0]
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

            rolling_y_val = rolling_y.values
            b, a, resid = self.regress(rolling_y_val, rolling_x,
                                       intercept=True, weight=weight, verbose=True)
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
    def DataClass(self):
        dc = DataPrepare(beginDate=self.beginDate, endDate=self.endDate)
        dc.get_Tushare_data()
        return dc

    def BackTest(self, data: pd.DataFrame, DataClass=None, **kwargs):
        '''
        :param data:
        :param DataClass:
        :return:
        '''
        if not DataClass: DataClass = self.DataClass
        data_reindex = reindex(data)
        score = info_lag(data2score(data_reindex), n_lag=1)
        # group beck test
        BTClass = simpleBT(DataClass.TICKER, DataClass.BENCH, **kwargs)
        fig, outMetrics, group_res = BTClass.groupBT(score)
        fig.show()  # 绘图
        print(outMetrics)  # 输出指标
