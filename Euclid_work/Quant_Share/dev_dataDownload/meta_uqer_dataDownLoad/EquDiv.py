"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/28 11:11
# @Author  : Euclid-Jie
# @File    : EquDiv.py
# @Desc    :
"""
from .base_package import *
from .rolling_save import rolling_save


def EquDiv(begin, end, **kwargs):
    """
    股票分红信息
    columns:
        secID	String	内部编码
        ticker	String	交易代码
        exchangeCD	String	交易市场。例如，XSHG-上海证券交易所；XSHE-深圳证券交易所。对应getSysCode.codeTypeID=10002。	查看参数可选
        secShortName	String	证券简称
        endDate	Date	分红年度截止日
        publishDate	Date	公告日期
        eventProcessCD	Int64	事件进程。例如，1-董事预案；3-股东大会否决。对应getSysCode.codeTypeID=20001。	查看参数可选
        perShareDivRatio	Double	每股送股比例
        perShareTransRatio	Double	每股转增股比例
        perCashDiv	Double	每股派现(税前)
        perCashDivAfTax	Double	每股派现(税后)
        currencyCD	String	货币种类。CNY-人民币元，对应getSysCode.codeTypeID=10004。	查看参数可选
        frPerCashDiv	Double	每股派现(税前)外币结算
        frPerCashDivAfTax	Double	每股派现(税后)外币结算
        frCurrencyCD	String	外币货币种类。例如，GBP-英镑；USD-美元。对应getSysCode.codeTypeID=10004。	查看参数可选
        recordDate	Date	股权登记日
        exDivDate	Date	除权除息日
        bLastTradeDate	Date	B股最后交易日
        payCashDate	Date	派现日
        bonusShareListDate	Date	红股上市日
        ftdAfExdiv	String	除权除息后交易首日
        sharesBfDiv	Double	分红前总股本
        sharesAfDiv	Double	分红后总股本
        baseShares	Double	分红股本基数
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.EquDivGet(
        ticker=kwargs["ticker"],
        beginDate=begin,
        endDate=end,
    )
    return data
