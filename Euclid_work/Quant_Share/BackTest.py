# -*- coding: utf-8 -*-
# @Time    : 2023/4/9 16:17
# @Author  : Euclid-Jie
# @File    : BackTest.py
import datetime
import math

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

from .EuclidGetData import get_data
from .Utils import *


class DataPrepare:
    def __init__(self, beginDate: str, endDate: str = None):
        # param init
        self.beginDate = beginDate
        self.endDate = endDate
        if self.endDate is None:
            self.endDate = datetime.date.today().strftime("%Y%m%d")

        # consts init
        self.TICKER = {}  # store ticker's data
        self.BENCH = {}  # store bench's data

        # Score init
        self.Score = None

        # tradeDate init
        self.tradeDateBase = tradeDate_info.loc[
            format_date(beginDate) : format_date(endDate)
        ]

    def get_bench_info(self):
        # bench nav
        bench_price_df = get_data(
            "MktIdx", begin=self.beginDate, end=self.endDate, ticker=["000300"]
        )
        bench_price_df["tradeDate"] = pd.to_datetime(bench_price_df["tradeDate"])
        bench_price_df = bench_price_df.sort_values("tradeDate", ascending=True)
        bench_price_df.set_index("tradeDate", inplace=True)
        self.BENCH["pct_chg"] = bench_price_df["CHGPct"]
        # TODO bench con code
        # self.BENCH['con_code']

    def get_price_data(self):
        price_df = get_data("MktEqud", begin=self.beginDate, end=self.endDate)
        for coli in [
            "openPrice",
            "closePrice",
            "turnoverRate",
            "isOpen",
            "vwap",
            "negMarketValue",
            "chgPct",
            "isOpen",
        ]:
            self.TICKER[coli] = reindex(
                price_df.pivot(index="tradeDate", columns="ticker", values=coli)
            )

    def get_score_data(self):
        # HKshszHold chg_pct
        HKHold = get_data("HKshszHold", begin=self.beginDate, end=self.endDate)
        HKHoldPct = HKHold.pivot(index="endDate", columns="ticker", values="partyPct")
        self.Score = reindex(HKHoldPct.pct_change(periods=1, axis=0))

    def get_limit_data(self):
        # suspend_type
        MktLimit_df = get_data("MktLimit", begin=self.beginDate, end=self.endDate)
        for coli in ["limitUpPrice", "limitDownPrice"]:
            self.TICKER[coli] = reindex(
                MktLimit_df.pivot(index="tradeDate", columns="ticker", values=coli)
            )

    def calc_score(self):
        # calc total value rank score
        self.Score = data2score(self.TICKER["negMarketValue"])
        # info lag, do not use future info
        self.Score = info_lag(self.Score, n_lag=1)

    def get_Tushare_data(self):
        print(">>> Waiting for Data prepare ...")
        self.get_bench_info()
        self.get_price_data()
        self.get_score_data()
        self.calc_score()
        # self.get_limit_data()


class simpleBT:
    def __init__(self, tickerData: dict, benchData: dict, **kwargs):
        self.benchData = benchData
        self.tickerData = tickerData
        self.ascending = kwargs.get("ascending", False)

    def backTest(self, Score, fee_rate=0.0008, group=1, dealPrice="close", plot=False):
        # format confirm
        if "Code" in Score.columns:  # if Score from local file, need to do this
            Score.set_index("Code", inplace=True)

        print(">>> Back Test staring ...")
        print("\t>>> the shape of score is {}".format(Score.shape))
        print("\t>>> rolling back test starting ...")
        # prepare rtn_before_trade and rtn_after_trade
        self.tickerData["Rtn"] = self.tickerData["chgPct"].copy()
        self.tickerData["Rtn"].fillna(0, inplace=True)
        if dealPrice == "close":
            rtn_before_trade = self.tickerData["Rtn"]
            rtn_after_trade = pd.DataFrame(
                np.zeros(rtn_before_trade.shape),
                index=self.tickerData["Rtn"].index,
                columns=self.tickerData["Rtn"].columns,
            )
        else:
            rtn_before_trade = (
                self.tickerData[dealPrice] / self.tickerData["closePrice"].shift(axis=0)
                - 1
            )
            rtn_after_trade = (
                self.tickerData["closePrice"] / self.tickerData[dealPrice] - 1
            )

            rtn_before_trade[pd.isnull(rtn_before_trade)] = 0
            rtn_after_trade[pd.isnull(rtn_after_trade)] = 0

        # back test init
        Score.dropna(axis=0, how="all", inplace=True)
        start_dt = Score.index[0]
        self.tickerData["Rtn"] = self.tickerData["Rtn"].loc[start_dt:]

        # init position, index is code, columns is tradeDate
        temp_pos = pd.Series(
            data=np.zeros(len(self.tickerData["Rtn"].columns)),
            index=self.tickerData["Rtn"].columns,
        )
        pos_out = pd.DataFrame(
            np.zeros(self.tickerData["Rtn"].shape),
            index=self.tickerData["Rtn"].index,
            columns=self.tickerData["Rtn"].columns,
        )
        # init empty fee、daily_rtn、turnover to store info, index is tradeDate
        fee = pd.Series(
            np.zeros(len(self.tickerData["Rtn"].index)),
            index=self.tickerData["Rtn"].index,
        )
        daily_rtn = fee.copy()
        turnover = fee.copy()

        for date in tqdm(self.tickerData["Rtn"].index.to_list()):
            # the rtn date should >= start date
            if date < start_dt:
                continue

            # calc the position holding rtn
            pos_rtn = (temp_pos * self.tickerData["Rtn"].loc[date]).sum()
            # init the rtn after trade as 0
            trade_rtn = 0

            if date in Score.index.to_list():
                # pos adjusted by rtn
                temp_pos = temp_pos * (1 + rtn_before_trade.loc[date])
                temp_pos = temp_pos / temp_pos.sum()
                temp_pos.fillna(0, inplace=True)

                # calc target position
                try:
                    this_pos = self.getGroupTargPost(
                        Score=Score.loc[date], group=group, ascending=self.ascending
                    )

                except ValueError:
                    this_pos = temp_pos

                # calc for buy and sell
                diff = this_pos - temp_pos
                temp_sell = diff < 0
                temp_buy = diff > 0
                # confirm tradable
                tradable = self.tickerData["isOpen"].loc[date] == 1
                # adjustment
                unfilled_buy = temp_buy[~tradable].sum()
                unfilled_sell = temp_sell[~tradable].sum()
                while True:
                    if unfilled_sell > 0:
                        if (temp_sell.sum() - unfilled_sell) == 0:  # 均无法卖出, 不换仓
                            diff = pd.Series(
                                data=np.zeros(len(self.tickerData["Rtn"].columns)),
                                index=self.tickerData["Rtn"].columns,
                            )
                            break
                        else:
                            diff[tradable & temp_sell] = diff[temp_sell].sum() / (
                                temp_sell.sum() - unfilled_sell
                            )

                    if unfilled_buy > 0:
                        if (temp_buy.sum() - unfilled_buy) == 0:  # 均无法买入, 需要进一步考虑
                            diff = pd.Series(
                                data=np.zeros(len(self.tickerData["Rtn"].columns)),
                                index=self.tickerData["Rtn"].columns,
                            )
                            break
                        else:
                            diff[tradable & temp_buy] = diff[temp_buy].sum() / (
                                temp_buy.sum() - unfilled_buy
                            )
                    break
                diff[~tradable] = 0
                fee.loc[date] = np.abs(diff).sum() * fee_rate  # 计算费用=(买入+卖出)*手续费率
                trade_rtn = (diff * rtn_after_trade.loc[date]).sum()  # 使用交易后的收益率更新收益
                temp_pos += diff  # 更新仓位
                turnover.loc[date] = np.abs(diff).sum()  # 存储当日的交易额

            else:  # not in Score
                temp_pos = temp_pos * (
                    1 + self.tickerData["Rtn"].loc[date]
                )  # 直接使用close/close_pre - 1收益率进行更新
                temp_pos = temp_pos / temp_pos.sum()
                temp_pos.fillna(0, inplace=True)

            pos_out.loc[date] = temp_pos  # 记录个股仓位
            daily_rtn.loc[date] = (
                pos_rtn + trade_rtn - fee.loc[date]
            )  # 每日收益=持仓收益+交易后的收益-手续费

        print(">>> Back Test done")
        print("-*" * 30)
        print(">>> calc and display risk metrics")
        # calc nav
        nav = (1 + daily_rtn).cumprod()  # 净值
        nav_out = nav.copy()  # 组合净值

        # 指数相关
        bench_rtn = self.benchData["pct_chg"].copy()
        # 计算指数净值
        bench_out = bench_rtn.loc[nav_out.index]
        bench_out.iloc[0] = 0
        bench_nav = (1 + bench_out).cumprod()

        # bkt returns
        pos_out = pos_out.loc[nav_out.index]  # 持仓
        alpha_out = nav_out / bench_nav  # 组合净值超额alpha = 组合净值/指数净值

        # cal risk-return indicators and store them in variable "results"
        result = self.curveanalysis(self, alpha_out)
        result["turnover"] = turnover.loc[start_dt:].mean()
        if group:
            result["group"] = group

        for key, val in result.items():
            if key in ["sharpe", "group"]:
                print("{:>20s}: {:.4f}".format(key, val))
            else:
                print("{:>20s}: {:.2%}".format(key, val))

        # plot
        if plot:
            fig, axis = plt.subplots(2, 1)
            nav_out.plot(ax=axis[0])  # 组合净值
            bench_nav.plot(ax=axis[0])  # 指数净值

            axis[0].set_title("nav")
            axis[0].legend(["port", "benchmark"])

            alpha_out.plot(ax=axis[1])  # 组合超额alpha
            axis[1].set_title("alpha nav")

            result["fig"] = fig

        return nav_out, pos_out, alpha_out, result

    @staticmethod
    def curveanalysis(cls, nav):
        """
        计算收益率曲线的风险指标
        :param cls:
        :param nav:
        :return:
        """
        rtn = np.diff(np.log(nav))
        result = {"totalrtn": nav[-1] / nav[0] - 1}
        number_of_years = (
            pd.to_datetime(nav.index[-1]).toordinal()
            - pd.to_datetime(nav.index[0]).toordinal()
        ) / 356
        result["alzdrtn"] = (result["totalrtn"] + 1) ** (1 / number_of_years) - 1
        result["stdev"] = rtn.std()
        result["vol"] = rtn.std() * np.sqrt(250)
        result["sharpe"] = result["alzdrtn"] / result["vol"]
        result["maxdown"] = 1 - 1 / math.exp(cls.maximum_drawdown(rtn))

        return result

    @staticmethod
    def getGroupTargPost(Score, group, ascending=True):
        """
        :param ascending:
        :param Score: 评分
        :param group: 评分分组
        :return: 目标仓位
        """
        labels = ["1", "2", "3", "4", "5"]
        if not ascending:
            labels = labels[::-1]
        TargPost = (
            pd.qcut(Score, 5, labels=labels, duplicates="drop") == str(group)
        ).astype(int)
        TargPost = TargPost / TargPost.sum()
        return TargPost

    @staticmethod
    def maximum_drawdown(data):
        """
        计算最大回撤
        :param data:
        :return:
        """
        min_all = 0
        sum_here = 0
        for x in data:
            sum_here += x
            if sum_here < min_all:
                min_all = sum_here
            elif sum_here >= 0:
                sum_here = 0
        return -min_all

    def groupBT(self, Score, **kwargs):
        group_res = {}
        for group in range(5):
            (nav, pos_out, alpha_nav, result) = self.backTest(
                Score,
                group=group + 1,
                dealPrice="vwap",
                fee_rate=kwargs.get("fee_rate", 0.0008),
            )
            group_res["{}".format(group + 1)] = (nav, pos_out, alpha_nav, result)

        # plot
        fig, axis = plt.subplots()
        for group in range(5):
            alpha_nav = group_res["{}".format(group + 1)][2]
            # print(alpha_nav[-1])
            alpha_nav.plot()  # 组合净值
        axis.set_title("Group alpha_nav")
        axis.legend(["Group_{}".format(i) for i in [1, 2, 3, 4, 5]])

        # calc metrics
        outMetrics = pd.DataFrame()
        for group in range(5):
            tmpMetrics = group_res["{}".format(group + 1)][3]
            tmpMetrics = pd.DataFrame(tmpMetrics, index=[group + 1])
            outMetrics = pd.concat([outMetrics, tmpMetrics], axis=0, ignore_index=True)

        return fig, outMetrics, group_res
