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
        net_cf_oper = net_cf_oper.groupby(pd.Grouper(freq='Q')).mean()
        df = self.pandas_parallelcal(net_cf_oper, myfunc=np.nansum, window=4)
        return df

    @cached_property
    def EPcut(self):
        NPCUT = self.NPCUT
        marketValue = self.marketValue
        NPCUT,marketValue = self.align_data([NPCUT,marketValue])
        df = NPCUT/marketValue
        return df

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
        cf_out_inv = self.cf_out_inv
        cf_out_inv = cf_out_inv.resample('D').asfreq().ffill()
        net_cf_oper = self.net_cf_oper
        net_cf_oper = net_cf_oper.resample('D').asfreq().ffill()
        cf_out_inv, net_cf_oper = self.align_data([cf_out_inv, net_cf_oper])
        df = net_cf_oper - cf_out_inv
        df = self.adjust(df)
        return df


