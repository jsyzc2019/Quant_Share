"""
# -*- coding: utf-8 -*-
# @Time    : 2023/4/22 15:33
# @Author  : Euclid-Jie
# @File    : FactorAnalysis.py
"""
import numpy as np
import pandas as pd

from Euclid_work.Quant_Share.Utils import printJson, reindex, info_lag, get_tradeDate, format_date, dataBase_root_path
from Euclid_work.Quant_Share.Euclid_get_data import get_data


def format_nav(nav):
    """
    处理净值序列为标准的pd.Series
    :param nav: 净值序列(起始值应为1), 支持 list, np.ndarray, pd.Series格式
    :return: pd.Series
    """
    if isinstance(nav, list):
        nav = pd.Series(nav)
    elif isinstance(nav, pd.DataFrame):
        if nav.shape[1] != 1:
            raise AttributeError("nav should have only one columns!")
        nav = nav.iloc[:, 0]
    elif not isinstance(nav, (pd.Series, np.ndarray)):
        raise TypeError("nav should be list, np.ndarray,pd.DataFrame, pd.Series!")

    if nav.iloc[0] != 1:
        raise ValueError("nav should  begin with 1!")
    return nav


def maxDrawDown(nav, absolutely=True):
    """
    最大回测计算
    :param nav: 净值序列(起始值为1), 支持 list, np.ndarray, pd.Series格式
    :param absolutely: 默认使用绝对(加减), False则为相对即使用乘除法
    :return: 回测值
    """
    nav = format_nav(nav)
    if absolutely:
        # 这种基于rtn的方式更快
        rtn = nav - 1
        min_all = 0
        sum_here = 0
        for x in rtn:
            sum_here += x
            if sum_here < min_all:
                min_all = sum_here
            elif sum_here >= 0:
                sum_here = 0
        return -min_all

    else:
        i = np.argmax((np.maximum.accumulate(nav) - nav) / np.maximum.accumulate(nav))
        j = np.argmax(nav[:i])
        return 1 - nav[i] / nav[j]


def get_Performance_analysis(nav, year_day=252):
    """
    计算净值曲线的绩效指标
    :param nav: 净值序列(起始值为1), 支持 list, np.ndarray, pd.Series格式
    :param year_day:
    :return:
    """
    nav = format_nav(nav)
    # 新高日期数 #突破能力
    max_T = 0
    # 循环净值
    for s in range(2, len(nav)):
        # 节点划分
        l = nav[:s]
        # 判断当前节点为最大值
        if l[-1] > l[:-1].max():
            # 新高日数+1
            max_T += 1

    # 净值新高天数占比
    max_day_rate = max_T / (len(nav) - 1)
    max_day_rate = round(max_day_rate * 100, 2)

    # 获取最终净值
    net_values = round(nav[-1], 4)

    # 计算算术年化收益率
    year_ret_mean = nav.pct_change().dropna().mean() * year_day
    year_ret_mean = round(year_ret_mean * 100, 2)

    # 计算几何年化收益率
    year_ret_sqrt = net_values ** (year_day / len(nav)) - 1
    year_ret_sqrt = round(year_ret_sqrt * 100, 2)

    # 计算年化波动率
    volitiy = nav.pct_change().dropna().std() * np.sqrt(year_day)
    volitiy = round(volitiy * 100, 2)

    # 计算夏普，无风险收益率记3%
    Sharpe = (year_ret_sqrt - 0.03) / volitiy
    Sharpe = round(Sharpe, 2)

    # 计算最大回撤
    downlow = maxDrawDown(nav)
    downlow = round(downlow * 100, 2)

    # 输出
    out = {
        'net_values': net_values,
        'year_ret_sqrt': year_ret_sqrt,
        'downlow': downlow,
        'Sharpe': Sharpe,
        'volitiy': volitiy,
        'max_day_rate': max_day_rate
    }
    return out


def get_new_stock_filter(Score, newly_listed_threshold=120):
    """
    新股剔除信息, 由数据限制, 仅对2015年 + newly_listed_threshold后的数据有效
    :param Score:
    :param newly_listed_threshold:
    :return:
    """
    # 速度慢, 单独提closePrice为文件
    # price_df = get_data('MktEqud')
    # closeP = reindex(price_df.pivot(index='tradeDate', columns='ticker', values='closePrice'))
    closeP = pd.read_csv("{}/dev_closeP_formated_wide.csv".format(dataBase_root_path), index_col='tradeDate')
    closeP = reindex(closeP)
    listed_days = (~pd.isnull(closeP)).cumsum()
    fit = listed_days < newly_listed_threshold
    fit = info_lag(fit, n_lag=1)
    if Score.empty:
        return fit
    else:
        Score = Score.copy()
        # TODO 改用纯交易日滞后
        tag = get_tradeDate(20150101, n=newly_listed_threshold)['tradeDate_fore']
        if format_date(Score.index[0]) <= tag:
            raise ResourceWarning("Score should start behind {}".format(tag.strftime("%Y-%m-%d")))
        fit = fit.reindex(Score.index, Score.columns)
        Score[fit] = np.nan
        return Score


def get_st_filter(Score=None):
    """
    处理ST股票信息
    :param Score: 传入空值则返回ST信息, 如果传入数据为Score, 则ST股对应的Score为会赋值为Nan
    :return:
    """
    SecST = get_data('SecST')
    SecST = SecST.pivot(index='tradeDate', columns='ticker', values='STflg')
    SecST = reindex(SecST, tradeDate=True).notna()
    SecST = info_lag(SecST, n_lag=1)
    if Score.empty:
        return SecST
    else:
        Score = Score.copy()
        fit = SecST.reindex(Score.index, Score.columns)
        Score[fit] = np.nan
        return Score


def get_suspended_filter(Score=None):
    """
    处理停牌信息
    :param Score:
    :return: 停牌为True或np.nan
    """
    # 直接使用MktEqud中的isopen即可
    isOpen = pd.read_csv("{}/dev_isOpen_formated_wide.csv".format(dataBase_root_path), index_col='tradeDate')
    isOpen = reindex(isOpen)
    fit = isOpen != 1
    if Score.empty:
        return fit
    else:
        Score = Score.copy()
        fit = fit.reindex(Score.index, Score.columns)
        Score[fit] = np.nan
        return Score


def get_limit_up_down_filter(Score=None):
    """
    处理涨跌停信息，涨停为1, 跌停为-1
    :param Score:
    :return: 涨跌停为True或np.nan
    """
    openP = pd.read_csv("{}/dev_openP_formated_wide.csv".format(dataBase_root_path), index_col='tradeDate')
    openP = reindex(openP)
    closeP = pd.read_csv("{}/dev_closeP_formated_wide.csv".format(dataBase_root_path), index_col='tradeDate')
    closeP = reindex(closeP)

    MktLimit = get_data('MktLimit')
    limitUp = reindex(MktLimit.pivot(index='tradeDate', columns='ticker', values='limitUpPrice'))
    limitDown = reindex(MktLimit.pivot(index='tradeDate', columns='ticker', values='limitDownPrice'))
    udStatus = (closeP == limitUp).astype(int) - (closeP == limitDown).astype(int)
    udStatus = udStatus.fillna(0)
    # udStatus[closeP != openP] = 0  # 非closeP交易, 实际上可以买卖
    if Score.empty:
        return udStatus
    else:
        fit = udStatus != 0
        Score = Score.copy()
        fit = fit.reindex(Score.index, Score.columns)
        Score[fit] = np.nan
        return Score


if __name__ == '__main__':
    score = pd.read_csv(r"D:\Share\Euclid_work\Src_Test\score.csv", index_col='tradeDate')

    get_limit_up_down_filter(score)
