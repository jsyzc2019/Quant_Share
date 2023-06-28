from typing import Union
from sklearn.linear_model import LinearRegression
from .BarraCNE6 import BARRA
from ..Utils import reindex
from functools import cached_property
import numpy as np
from datetime import date
import pandas as pd

class FactorScore(BARRA):

    def __init__(self, beginDate: str = None, endDate: str = None):
        endDate = endDate if endDate else date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)

    @cached_property
    def net_operate_cash_flow_ttm(self):
        """
        经营活动现金流量净额TTM
        过去12个月 经营活动产生的现金流量净值(net_cf_oper) 之和
        :return:
        """
        net_cf_oper = self.net_cf_oper
        net_cf_oper = net_cf_oper.groupby(pd.Grouper(freq='Q')).mean().fillna(0)
        df = self.pandas_parallelcal(net_cf_oper, myfunc=np.nansum, window=4)
        return df

    @cached_property
    def NPCUT_ttm(self):
        NPCUT = self.NPCUT
        NPCUT = NPCUT.groupby(pd.Grouper(freq='Q')).mean().fillna(0)
        NPCUT_ttm = self.pandas_parallelcal(NPCUT, myfunc=np.nansum, window=4)
        return NPCUT_ttm

    @cached_property
    def EPcut(self):
        NPCUT_ttm = self.NPCUT_ttm
        NPCUT_ttm = NPCUT_ttm.resample('D').asfreq().ffill()
        marketValue = self.marketValue
        NPCUT_ttm,marketValue = self.align_data([NPCUT_ttm,marketValue])
        df = NPCUT_ttm/marketValue
        df = self.adjust(df)
        return df

    @cached_property
    def net_prof_ttm(self):
        net_prof = self.net_prof
        net_prof = net_prof.groupby(pd.Grouper(freq='Q')).mean().fillna(0)
        net_prof_ttm = self.pandas_parallelcal(net_prof, myfunc=np.nansum, window=4)
        return net_prof_ttm

    @cached_property
    def OCFP(self):
        net_operate_cash_flow_ttm = self.net_operate_cash_flow_ttm
        net_operate_cash_flow_ttm = net_operate_cash_flow_ttm.resample('D').asfreq().ffill()
        marketValue = self.marketValue
        net_operate_cash_flow_ttm, marketValue = self.align_data([net_operate_cash_flow_ttm,marketValue])
        df = net_operate_cash_flow_ttm/marketValue
        df = self.adjust(df)
        return df

    @cached_property
    def FCFP(self):
        """
        自由现金流 = 经营活动现金流量净额－投资活动现金流出
        :return:
        """
        net_cf_inv = self.net_cf_inv
        net_cf_inv = net_cf_inv.resample('D').asfreq().ffill()
        net_cf_oper = self.net_cf_oper
        net_cf_oper = net_cf_oper.resample('D').asfreq().ffill()
        cf_out_inv, net_cf_oper = self.align_data([net_cf_inv, net_cf_oper])
        df = net_cf_oper - net_cf_inv
        df = self.adjust(df)
        return df

    @cached_property
    def G_PE(self):
        net_prof_ttm = self.net_prof_ttm
        net_prof_ttm = net_prof_ttm.resample('D').asfreq().ffill()
        PETTM = self.PETTM
        PETTM, net_prof_ttm = self.align_data([PETTM, net_prof_ttm])
        df = net_prof_ttm/PETTM
        df = self.adjust(df)
        return df

    def get_dummy_industry(self, industry_data, trade_date):
        if isinstance(trade_date, str): trade_date = pd.to_datetime(trade_date)
        df_in_date = industry_data[industry_data.index == trade_date]
        df_in_date = df_in_date.reset_index()
        df_in_date = df_in_date.melt(id_vars=['date'], var_name='ticker', value_name='industryName')
        X_industry = pd.get_dummies(df_in_date, columns=['industryName'], drop_first=True)
        X_industry = X_industry.drop('date', axis=1)
        return X_industry

    def concat_industry_market(self, industry, trade_date, market=None):
        X_industry = self.get_dummy_industry(industry, trade_date)
        if market is not None:
            mk_in_date = market[market.index == trade_date]
            if len(mk_in_date) == 0:
                return X_industry.drop('ticker', axis=1)
            mk_in_date = mk_in_date.reset_index()
            mk_in_date = mk_in_date.melt(id_vars=['tradeDate'], var_name='ticker', value_name='LNCAP')
            mk_in_date = mk_in_date.drop('tradeDate', axis=1)
            X_all = mk_in_date.merge(X_industry, on='ticker', how='outer')
            X_all = X_all.drop('ticker', axis=1)
            return X_all
        else:
            X_industry = X_industry.drop('ticker', axis=1)
            return X_industry

    def neutral_process(self, y:pd.Series):
        trade_date = y.name
        X = self.concat_industry_market(industry=self.indus_data, market=self.market_data, trade_date=trade_date)
        # _,_,resid = self.regress(y.values, X.values, intercept=True, verbose=True)
        y = y.fillna(0)
        X = X.fillna(0)
        model = LinearRegression().fit(X.values, y.values)
        resid = y - model.predict(X.values)
        return resid

    def neutralization(self, factor:Union[pd.DataFrame, str], industry=True, market=True):
        if isinstance(factor, str):
            factor = getattr(self, factor)
        factor = reindex(factor).dropna(axis=0, how='all')
        factor = factor.replace(np.inf, np.nan)
        factor = factor.fillna(factor.mean(skipna=True))

        assert industry or market
        if industry:
            self.indus_data = reindex(self.industry).fillna('未知板块')
        else:
            self.indus_data = None
        if market:
            self.market_data = reindex(self.LNCAP)
            self.market_data = self.market_data.fillna(self.market_data.mean(skipna=True))
        else:
            self.market_data = None

        if industry and market:
            factor, self.indus_data, self.market_data = self.align_data([factor, self.indus_data, self.market_data])
        elif industry:
            factor, self.indus_data = self.align_data([factor, self.indus_data])
        elif market:
            factor, self.market_data = self.align_data([factor, self.market_data])

        fator_nual = factor.apply(self.neutral_process, axis=1)
        return fator_nual

    def ScoretoGroup(self, Score, num=5, inverse=False):
        '''
        :param Score:
        :param num:
        :param inverse:
        :return:
        '''
        tags = [str(i) for i in range(1,num+1)]
        if inverse:
            tags.reverse()
        def func(Score, num, tags, inverse):
            try:
                group = pd.qcut(Score, num, labels=tags, duplicates='drop')
                return group
            except ValueError as e:
                print(e)
                print(f"Try to set true tags' number = {len(tags) - 1}")
                tags = [str(i) for i in range(1, len(tags))]
                if inverse:
                    tags.reverse()
                return func(Score, num, tags, inverse)
        group = func(Score, num, tags, inverse)

        res = {}
        tickers = Score.index
        for tg in tags:
            res[tg] = tickers[group == tg]
        return res




