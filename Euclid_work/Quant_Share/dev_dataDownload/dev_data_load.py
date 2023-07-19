"""
# -*- coding: utf-8 -*-
# @Time    : 2023/4/22 19:35
# @Author  : Euclid-Jie
# @File    : dev_data_load.py
"""
import pandas as pd
from tqdm import tqdm
from uqer import DataAPI, Client
from Euclid_work.Quant_Share.Utils import (
    stockNumList,
    format_date,
    save_data_h5,
    dataBase_root_path,
    extend_date_span,
)


def MktIdx(begin, end, **kwargs):
    """
    指数日行情
    :param begin:
    :param end:
    :param kwargs: indexID = get_data("SecID_IDX_info")['secID'].to_list()
    :return:
    """
    if "indexID" not in kwargs.keys():
        raise AttributeError("indexID should in kwargs!")
    data = DataAPI.MktIdxdGet(
        indexID=kwargs["indexID"],
        ticker="",
        tradeDate="",
        beginDate=begin,
        endDate=end,
        exchangeCD=["XSHE", "XSHG"],
        field="",
        pandas="1",
    )
    return data


def HKshszHold(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.HKshszHoldGet(
        secID="",
        ticker=kwargs["ticker"],
        tradeCD="1",
        ticketCode="",
        partyName="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def MktLimit(begin, end, **kwargs):
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.MktLimitGet(
        secID="",
        ticker=kwargs["ticker"],
        tradeDate="",
        exchangeCD="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def ResConIndustryCitic(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    :return:
    """
    if "induID" not in kwargs.keys():
        raise AttributeError("induID should in kwargs!")
    data = DataAPI.ResConIndustryCiticGet(
        beginDate=begin,
        endDate=end,
        induID=kwargs["induID"],
        indexID="",
        indexCode="",
        induLevel="",
        field="",
        pandas="1",
    )
    return data


def ResConIndustryCiticFy12(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    :return:
    """
    if "induID" not in kwargs.keys():
        raise AttributeError("induID should in kwargs!")
    data = DataAPI.ResConIndustryCiticFy12Get(
        beginDate=begin,
        endDate=end,
        induID=kwargs["induID"],
        indexID="",
        indexCode="",
        induLevel="",
        field="",
        pandas="1",
    )
    return data


def ResConIndustrySw(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010321"]['industryID'].to_list()]
    :return:
    """
    if "induID" not in kwargs.keys():
        raise AttributeError("induID should in kwargs!")
    data = DataAPI.ResConIndustrySwGet(
        beginDate=begin,
        endDate=end,
        induID=kwargs["induID"],
        indexID="",
        indexCode="",
        induLevel="",
        field="",
        pandas="1",
    )
    return data


def ResConIndustrySwFy12(begin, end, **kwargs):
    if "induID" not in kwargs.keys():
        raise AttributeError("induID should in kwargs!")
    data = DataAPI.ResConIndustrySwFy12Get(
        beginDate=begin,
        endDate=end,
        induID=kwargs["induID"],
        indexID="",
        indexCode="",
        induLevel="",
        field="",
        pandas="1",
    )
    return data


def ResConSecCoredata(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecCoredataGet(
        secCode=kwargs["secCode"],
        secName="",
        repForeDate="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def ResConSecCorederi(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecCorederiGet(
        secCode=kwargs["secCode"],
        secName="",
        repForeDate="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def ResConSecFy12(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecFy12Get(
        secCode=kwargs["secCode"],
        secName="",
        repForeDate="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def ResConSecReportHeat(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecReportHeatGet(
        secCode=kwargs["secCode"],
        secName="",
        repForeDate="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def RMExposureDay(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.RMExposureDayGet(
        secID="",
        ticker=kwargs["ticker"],
        tradeDate="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def SecST(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.SecSTGet(
        beginDate=begin,
        endDate=end,
        secID="",
        ticker=kwargs["ticker"],
        field="",
        pandas="1",
    )
    return data


def SecHalt(begin, end, **kwargs):
    """
    沪深证券停复牌
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.SecHaltGet(
        secID="",
        ticker=kwargs["ticker"],
        beginDate=begin,
        endDate=end,
        listStatusCD="",
        assetClass="",
        field="",
        pandas="1",
    )
    return data


def FdmtIndiRtnPit(begin, end, **kwargs):
    """
    财务指标—盈利能力 (Point in time)
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.FdmtIndiRtnPitGet(
        ticker=kwargs["ticker"],
        secID="",
        beginDate=begin,
        endDate=end,
        beginYear="",
        endYear="",
        reportType="",
        publishDateEnd="",
        publishDateBegin="",
        field="",
        pandas="1",
    )
    return data


def MktEqudEval(begin, end, **kwargs):
    """
    沪深估值信息
    DataAPI.MktEqudEvalGet
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.MktEqudEvalGet(
        ticker=kwargs["ticker"],
        secID="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def FdmtIndiPSPit(begin, end, **kwargs):
    """
    财务指标—每股 (Point in time)
    DataAPI.FdmtIndiPSPitGet
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.FdmtIndiPSPitGet(
        ticker=kwargs["ticker"],
        secID="",
        beginDate=begin,
        endDate=end,
        field="",
        pandas="1",
    )
    return data


def info_save(func, tableName, subPath, **kwargs):
    # reWrite or not
    reWrite = False
    if "reWrite" in kwargs.keys():
        reWrite = kwargs["reWrite"]

    data = func(**kwargs)
    save_data_h5(data, tableName, subPath=subPath, reWrite=reWrite)


def SecID_IDX_info():
    data = DataAPI.SecIDGet(
        partyID="",
        ticker="",
        cnSpell="",
        assetClass="IDX",
        exchangeCD=["XSHE", "XSHG"],
        listStatusCD="",
        field="",
        pandas="1",
    )
    return data


def TradeCal(**kwargs):
    if ("begin" not in kwargs.keys()) or ("end" not in kwargs.keys()):
        raise AttributeError("begin and end should be assigned")
    data = DataAPI.TradeCalGet(
        exchangeCD=["XSHG,XSHE"],
        beginDate=kwargs["begin"],
        endDate=kwargs["end"],
        isOpen="",
        field="",
        pandas="1",
    )
    return data


if __name__ == "__main__":
    # 通联登录
    with open("token.txt", "rt", encoding="utf-8") as f:
        token = f.read().strip()
    client = Client(token=token)

    # induID = get_data('IndustryID_info')
    # induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    # induID_Sw = [int(i) for i in induID[induID['industryVersionCD'] == "010321"]['industryID'].to_list()]

    # 优矿数据
    for tableName in ["FdmtIndiPSPit"]:
        print(tableName)
        for year in range(2015, 2024):
            begin = "{}0101".format(year)
            end = "{}1231".format(year)
            rolling_save(
                eval(tableName),
                tableName,
                begin,
                end,
                freq="q",
                monthlyStack=False,
                subPath="{}/{}".format(dataBase_root_path, tableName),
                ticker=stockNumList,
                reWrite=True,
            )
