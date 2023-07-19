# -*- coding: utf-8 -*-
# @Time    : 2023/4/16 15:53
# @Author  : Euclid-Jie
# @File    : FactorTest.py
import numpy as np
import pandas as pd
import scipy.stats as st

from .BackTest import DataPrepare, reindex
from .EuclidGetData import get_data
import datetime


class FactorTest(DataPrepare):
    def __init__(self, beginDate: str, endDate: str = None):
        endDate = endDate if endDate else datetime.date.today().strftime("%Y%m%d")
        super().__init__(beginDate, endDate)
        self.get_rtn_data()

    def get_rtn_data(self):
        price_df = get_data("MktEqud", begin=self.beginDate, end=self.endDate)
        self.TICKER["rtn"] = reindex(
            price_df.pivot(index="tradeDate", columns="ticker", values="chgPct")
        )

    def calc_IC(self, Score):
        Score.dropna(how="all", axis=0, inplace=True)
        rankIC = pd.DataFrame(
            np.zeros((Score.shape[0], 1)), index=Score.index, columns=["rankIC"]
        )
        for index, values in Score.iterrows():
            if index in self.TICKER["rtn"].index:
                tmp = pd.concat(
                    [values, self.TICKER["rtn"].loc[index]], axis=1, ignore_index=True
                )
                tmp.dropna(axis=0, inplace=True)
                tmp = tmp.rank(axis=0)
                # rank
                res = st.spearmanr(tmp)
                if res.pvalue > 0.05:
                    rankIC.loc[index] = 0
                else:
                    rankIC.loc[index] = res.statistic
        rankIC.dropna(axis=0, inplace=True)
        IR = (rankIC.mean()) / rankIC.std()

        return rankIC, IR
