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
from tqdm import tqdm
warnings.filterwarnings('ignore')

import seaborn as sns
from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo
import matplotlib.pylab as plt
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
        beta, alpha, sigma = self.rolling_regress(Y, X, window=window,
                                                 half_life=half_life)
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
            rolling_y = pd.DataFrame(rolling_y, columns=y.columns,
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

class MultiFactor(FactorBase):

    def __int__(self):
        pass

    @staticmethod
    def prepare(obj, names:list[str]) -> list[pd.DataFrame]:
        with tqdm(names) as t:
            res = []
            for name in t:
                t.set_description(f"Preparing {name}")
                res.append(getattr(obj, name))
        return res

    @staticmethod
    def retrive_ticker(ticker, factor_lst, names=None):
        res = pd.concat([df[ticker] for df in factor_lst], axis=1)
        if names is None:
            names = []
            suffix = 1
            for df in factor_lst:
                try:
                    name = df.name
                    names.append(name)
                except AttributeError:
                    names.append('unknown factor ' + str(suffix))
                    suffix += 1
        res.columns = names
        return res

    @staticmethod
    def analysis(mul:pd.DataFrame, num, rotation='varimax'):
        assert 0 < num < mul.shape[1]
        chi_square_value, p_value = calculate_bartlett_sphericity(mul)
        if p_value <= 0.05:
            print(f"p-Val:{p_value}: There is a certain correlation between the variables")
        else:
            print(f"p-Val:{p_value}: There is NOT a certain correlation between the variables")

        kmo_all, kmo_model = calculate_kmo(mul)
        if kmo_model > 0.6:
            print(f"KMO:{kmo_model}: Value of KMO larger than 0.6 is considered adequate")
        else:
            print(f"KMO:{kmo_model}: Value of KMO less than 0.6 is considered inadequate")

        faa = FactorAnalyzer(mul.shape[1], rotation=None)
        faa.fit(mul)
        # 得到特征值ev、特征向量v
        ev, v = faa.get_eigenvalues()

        plt.figure(figsize=(8,8))
        # 同样的数据绘制散点图和折线图
        plt.scatter(range(1, mul.shape[1] + 1), ev)
        plt.plot(range(1, mul.shape[1] + 1), ev)

        # 显示图的标题和xy轴的名字
        # 最好使用英文，中文可能乱码
        plt.title("Scree Plot")
        plt.xlabel("Factors")
        plt.ylabel("Eigenvalue")

        plt.grid()  # 显示网格
        plt.show()  # 显示图形

        faa_sub = FactorAnalyzer(num, rotation=rotation)
        faa_sub.fit(mul)

        with pd.option_context('expand_frame_repr', False, 'display.max_rows', None):
            print('因子方差'.center(30,'-'))
            print(pd.DataFrame(faa_sub.get_communalities(), index=mul.columns,columns=['Std']))
            print('因子特征值'.center(30, '-'))
            print(pd.DataFrame(faa_sub.get_eigenvalues(),
                               index=['original_eigen_values(原始特征值)','common_factor_eigen_values(公共因子特征值)'],
                               columns=mul.columns))
            print('成分矩阵'.center(30, '-'))
            print(pd.DataFrame(faa_sub.loadings_, index=mul.columns))
            print('因子贡献率'.center(30, '-'))
            print(pd.DataFrame(faa_sub.get_factor_variance(), index=['SS Loadings', 'Proportion Var', 'Cumulative Var']))

        df = pd.DataFrame(np.abs(faa_sub.loadings_), index=mul.columns)
        plt.figure(figsize=(8, 8))
        ax = sns.heatmap(df, annot=True, cmap="BuPu")
        # 设置y轴字体大小
        ax.yaxis.set_tick_params(labelsize=15)
        plt.title("Factor Analysis", fontsize="xx-large")
        # 设置y轴标签
        plt.ylabel("Sepal Width", fontsize="xx-large")
        # 显示图片
        plt.show()

        return faa_sub
