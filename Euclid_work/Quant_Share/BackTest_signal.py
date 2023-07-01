"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/27 22:16
# @Author  : Euclid-Jie
# @File    : BackTest_signal.py
"""
import numpy as np

from .BackTest_Meng import DataPrepare, simpleBT
from .Utils import get_tradeDate
import matplotlib.pyplot as plt


class DataPrepare_signal(DataPrepare):
    def __int__(self, beginDate: str, endDate: str = None, bench: str = None):
        super.__init__(beginDate, endDate, bench)

    def get_Tushare_data(self):
        print(">>> Waiting for Data prepare ...")
        self.get_bench_info()
        self.get_price_data()
        self.calc_score()


class simpleBT_signal(simpleBT):
    def __int__(self, tickerData: dict, benchData: dict, negValueAdjust: bool = True):
        super.__init__(tickerData, benchData, negValueAdjust)
        # assert "con_code" in benchData.keys() and "pct_chg" in benchData.keys()

    @staticmethod
    def getGroupTargPost(Score, group=None, ascending=True):
        """
        仅根据因子值大小设置仓位
        :param Score: 评分
        :param group: 评分分组
        :param ascending: if true, the score bigger, the pos bigger, default True
        :return: 目标仓位
        """
        # 清理Score
        # TODO(@Euclid) 形成新的Utils.Clean，适配Array(1D,2D), pd.Series, pd.DataFrame
        Score.loc[
            (Score < 0).values | (Score.isna()).values | (Score == np.Inf).values
        ] = 0

        # 调整仓位和为1
        assert Score.sum() > 0
        TargPost = Score / Score.sum()
        return TargPost

    def main_back_test(self, Score, **kwargs):
        Score.dropna(axis=0, how="all", inplace=True)
        metric_begin = get_tradeDate(kwargs.get("metric_begin", Score.index[0]), 0)
        plot_begin = get_tradeDate(kwargs.get("plot_begin", Score.index[0]), 0)
        # 传入的group参数为None
        (nav, pos_out, alpha_nav, result, daily_rtn) = self.backTest(
            Score.loc[metric_begin:], dealPrice="vwap", **kwargs
        )

        # plot
        fig, axis = plt.subplots()
        self.get_nav_data_2_plot(nav.loc[plot_begin:]).plot()  # 组合净值
        axis.set_title("nav VS bench_nav")
        bench_nav = nav / alpha_nav
        self.get_nav_data_2_plot(bench_nav.loc[plot_begin:]).plot()  # bench
        axis.legend(["nav", "bench_nav"])

        return fig, result, nav, pos_out, alpha_nav, daily_rtn
