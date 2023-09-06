"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:27
# @Author  : Euclid-Jie
# @File    : MktEqud.py
"""
from .base_package import *


def MktEqudAdj(begin, end, **kwargs):
    """
    沪深股票前复权行情
    columns:
        secID	String	通联编制的证券编码，可使用getSecID获取
        ticker	String	通用交易代码
        secShortName	String	证券简称
        exchangeCD	String	通联编制的交易市场编码	查看参数可选
        tradeDate	Date	交易日期
        preClosePrice	Double	昨收盘(前复权)
        actPreClosePrice	Double	昨收盘(未复权)
        openPrice	Double	今开盘(前复权)
        highestPrice	Double	最高价(前复权)
        lowestPrice	Double	最低价(前复权)
        closePrice	Double	今收盘(前复权)
        turnoverVol	Double	成交量(前复权)
        negMarketValue	Double	流通市值，收盘价*无限售流通股数
        dealAmount	Int32	成交笔数
        turnoverRate	Double	日换手率
        accumAdjFactor	Double	累积前复权因子，前复权价=未复权价*对应的累积前复权因子。最新一次除权除息日至最新行情期间的价格不需要进行任何的调整，该期间前复权因子都是1。
        turnoverValue	Double	成交金额
        marketValue	Double	总市值，收盘价*总股本数
        isOpen	Int32	股票今日是否开盘标记：0-未开盘，1-交易日
        vwap	Double	VWAP，成交金额/成交量*累积前复权因子
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.MktEqudAdjGet(
        secID="",
        ticker=kwargs["ticker"],
        tradeDate="",
        beginDate=begin,
        endDate=end,
        isOpen="",
        field="",
        pandas="1",
    )
    return data
