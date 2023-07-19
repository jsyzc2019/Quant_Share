"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:27
# @Author  : Euclid-Jie
# @File    : MktEqud.py
"""
from .base_package import *
from .rolling_save import rolling_save


def MktEqud(begin, end, **kwargs):
    """
    沪深股票日行情
    columns:
        secID	str	通联编制的证券编码，可使用DataAPI.SecIDGet获取
        ticker	str	通用交易代码
        secShortName	str	证券简称
        exchangeCD	str	通联编制的交易市场编码	查看
        tradeDate	str	交易日期
        preClosePrice	float	昨收盘(前复权)
        actPreClosePrice	float	实际昨收盘价(未复权)
        openPrice	float	开盘价
        highestPrice	float	最高价
        lowestPrice	float	最低价
        closePrice	float	收盘价
        turnoverVol	float	成交量
        turnoverValue	float	成交金额，A股单位为元，B股单位为美元或港币
        dealAmount	int	成交笔数
        turnoverRate	float	日换手率，成交量/无限售流通股数
        accumAdjFactor	float	累积前复权因子，前复权价=未复权价*累积前复权因子。前复权是对历史行情进行调整，除权除息当日的行情无需调整。最近一次除权除息日至最新交易日期间的价格也无需调整，该期间前复权因子等于1。
        negMarketValue	float	流通市值，收盘价*无限售流通股数
        marketValue	float	总市值，收盘价*总股本数
        chgPct	float	涨跌幅，收盘价/昨收盘价-1
        PE	float	滚动市盈率，即市盈率TTM，总市值/归属于母公司所有者的净利润TTM
        PE1	float	动态市盈率，总市值/归属于母公司所有者的净利润（最新一期财报年化）
        PB	float	市净率，总市值/归属于母公司所有者权益合计
        isOpen	int	股票今日是否开盘标记：0-未开盘，1-交易日
        vwap	float	VWAP，成交金额/成交量
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.MktEqudGet(
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


def MktEqud_update(upDateBegin, endDate="20231231"):
    rolling_save(
        MktEqud,
        "MktEqud",
        upDateBegin,
        endDate,
        freq="q",
        subPath="{}/MktEqud".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=stockNumList,
    )
