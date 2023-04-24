"""
# -*- coding: utf-8 -*-
# @Time    : 2023/4/22 19:35
# @Author  : Euclid-Jie
# @File    : dev_data_load.py
"""
import pandas as pd
from tqdm import tqdm
from uqer import DataAPI, Client
from Euclid_work.Quant_Share.Euclid_get_data import get_data
from Euclid_work.Quant_Share.Utils import stockNumList, format_date, save_data_h5, dataBase_root_path


def MktIdx(begin, end, **kwargs):
    """
    指数日行情
    :param begin:
    :param end:
    :param kwargs: indexID = get_data("SecID_IDX_info")['secID'].to_list()
    :return:
    """
    if 'indexID' not in kwargs.keys():
        raise AttributeError('indexID should in kwargs!')
    data = DataAPI.MktIdxdGet(indexID=kwargs['indexID'], ticker=u"", tradeDate=u"", beginDate=begin, endDate=end, exchangeCD=["XSHE", "XSHG"], field=u"", pandas="1")
    return data


def HKshszHold(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.HKshszHoldGet(secID=u"", ticker=kwargs['ticker'], tradeCD=u"1", ticketCode=u"", partyName=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def MktEqud(begin, end, **kwargs):
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.MktEqudGet(secID=u"", ticker=kwargs['ticker'], tradeDate=u"", beginDate=begin, endDate=end, isOpen="", field=u"", pandas="1")
    return data


def MktLimit(begin, end, **kwargs):
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.MktLimitGet(secID=u"", ticker=kwargs['ticker'], tradeDate=u"", exchangeCD=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def ResConIndustryCitic(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    :return:
    """
    if 'induID' not in kwargs.keys():
        raise AttributeError('induID should in kwargs!')
    data = DataAPI.ResConIndustryCiticGet(beginDate=begin, endDate=end, induID=kwargs['induID'], indexID=u"", indexCode=u"", induLevel=u"", field=u"", pandas="1")
    return data


def ResConIndustryCiticFy12(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    :return:
    """
    if 'induID' not in kwargs.keys():
        raise AttributeError('induID should in kwargs!')
    data = DataAPI.ResConIndustryCiticFy12Get(beginDate=begin, endDate=end, induID=kwargs['induID'], indexID=u"", indexCode=u"", induLevel=u"", field=u"", pandas="1")
    return data


def ResConIndustrySw(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: induID = get_data('IndustryID_info')
    induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010321"]['industryID'].to_list()]
    :return:
    """
    if 'induID' not in kwargs.keys():
        raise AttributeError('induID should in kwargs!')
    data = DataAPI.ResConIndustrySwGet(beginDate=begin, endDate=end, induID=kwargs['induID'], indexID=u"", indexCode=u"", induLevel=u"", field=u"", pandas="1")
    return data


def ResConIndustrySwFy12(begin, end, **kwargs):
    if 'induID' not in kwargs.keys():
        raise AttributeError('induID should in kwargs!')
    data = DataAPI.ResConIndustrySwFy12Get(beginDate=begin, endDate=end, induID=kwargs['induID'], indexID=u"", indexCode=u"", induLevel=u"", field=u"", pandas="1")
    return data


def ResConSecCoredata(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if 'secCode' not in kwargs.keys():
        raise AttributeError('secCode should in kwargs!')
    data = DataAPI.ResConSecCoredataGet(secCode=kwargs['secCode'], secName=u"", repForeDate=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def ResConSecCorederi(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if 'secCode' not in kwargs.keys():
        raise AttributeError('secCode should in kwargs!')
    data = DataAPI.ResConSecCorederiGet(secCode=kwargs['secCode'], secName=u"", repForeDate=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def ResConSecFy12(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if 'secCode' not in kwargs.keys():
        raise AttributeError('secCode should in kwargs!')
    data = DataAPI.ResConSecFy12Get(secCode=kwargs['secCode'], secName=u"", repForeDate=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def ResConSecReportHeat(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: secCode = stockNumList
    :return:
    """
    if 'secCode' not in kwargs.keys():
        raise AttributeError('secCode should in kwargs!')
    data = DataAPI.ResConSecReportHeatGet(secCode=kwargs['secCode'], secName=u"", repForeDate=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def RMExposureDay(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.RMExposureDayGet(secID=u"", ticker=kwargs['ticker'], tradeDate=u"", beginDate=begin, endDate=end, field=u"", pandas="1")
    return data


def SecST(begin, end, **kwargs):
    """

    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.SecSTGet(beginDate=begin, endDate=end, secID=u"", ticker=kwargs['ticker'], field=u"", pandas="1")
    return data


def SecHalt(begin, end, **kwargs):
    """
    沪深证券停复牌
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.SecHaltGet(secID=u"", ticker=kwargs['ticker'], beginDate=begin, endDate=end, listStatusCD="", assetClass="", field=u"", pandas="1")
    return data


def get_span_list(begin, end, freq=None):
    begin = format_date(begin)
    end = format_date(end)
    # pd.tseries.offsets 与 pd.offsets 使用无差异
    if freq in ['Y', 'y']:
        if not pd.offsets.DateOffset().is_year_start(begin):
            begin = begin - pd.offsets.YearBegin(n=1)
        if not pd.offsets.DateOffset().is_year_end(end):
            end = end + pd.offsets.YearEnd(n=1)

        span_end_list = pd.date_range(begin, end, freq='y')
        span_list = [(span_end - pd.offsets.YearBegin(1), span_end, "Y{}".format(span_end.year)) for span_end in span_end_list]
        return span_list

    elif freq in ['M', 'm']:
        if not pd.DateOffset().is_month_start(begin):
            begin = begin - pd.offsets.MonthBegin(n=1)
        if not pd.offsets.DateOffset().is_month_end(end):
            end = end + pd.offsets.MonthEnd(n=1)

        span_end_list = pd.date_range(begin, end, freq='m')
        span_list = [(span_end - pd.offsets.MonthBegin(1), span_end, "Y{}_M{}".format(span_end.year, span_end.month)) for span_end in span_end_list]
        return span_list

    elif freq in ['Q', 'q']:
        if not pd.DateOffset().is_quarter_start(begin):
            begin = begin - pd.offsets.QuarterBegin(n=1, startingMonth=1)
        if not pd.offsets.DateOffset().is_quarter_end(end):
            end = end + pd.offsets.QuarterEnd(n=1)

        span_end_list = pd.date_range(begin, end, freq='q')
        span_list = [(span_end - pd.offsets.QuarterBegin(n=1, startingMonth=1), span_end, "Y{}_Q{}".format(span_end.year, span_end.quarter)) for span_end in span_end_list]
        return span_list


def rolling_save(func, tableName, begin, end, freq, subPath, **kwargs):
    spanList = get_span_list(begin, end, freq=freq)

    # reWrite or not
    reWrite = False
    if 'reWrite' in kwargs.keys():
        reWrite = kwargs['reWrite']

    # monthly Stack or not
    monthlyStack = False
    if 'monthlyStack' in kwargs.keys():
        monthlyStack = kwargs['monthlyStack']

    if monthlyStack and freq in ['Q', 'q', 'Y', 'y']:
        with tqdm(spanList) as t:
            for begin_day, end_day, tag in t:
                t.set_postfix({"span": "{}-{}".format(begin_day, end_day)})
                data = pd.DataFrame()
                for _begin, _end, __ in get_span_list(begin_day, end_day, freq='m'):
                    tmpData = func(_begin.strftime("%Y%m%d"), _end.strftime("%Y%m%d"), **kwargs)
                    data = pd.concat([data, tmpData], axis=0, ignore_index=True)
                save_data_h5(data, name='{}_{}'.format(tableName, tag), subPath=subPath, reWrite=reWrite)

    else:  # (monthlyStack=Ture and freq in ['M', 'm'])  or monthlyStack=False
        with tqdm(spanList) as t:
            for begin_day, end_day, tag in t:
                t.set_postfix({"span": "{}-{}".format(begin_day, end_day)})
                data = func(begin_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d"), **kwargs)
                save_data_h5(data, name='{}_{}'.format(tableName, tag), subPath=subPath, reWrite=reWrite)


def info_save(func, tableName, subPath, **kwargs):
    # reWrite or not
    reWrite = False
    if 'reWrite' in kwargs.keys():
        reWrite = kwargs['reWrite']

    data = func(**kwargs)
    save_data_h5(data, tableName, subPath=subPath, reWrite=reWrite)


def SecID_IDX_info():
    data = DataAPI.SecIDGet(partyID=u"", ticker=u"", cnSpell=u"", assetClass=u"IDX", exchangeCD=["XSHE", "XSHG"], listStatusCD="", field=u"", pandas="1")
    return data


def TradeCal(**kwargs):
    if ('begin' not in kwargs.keys()) or ('end' not in kwargs.keys()):
        raise AttributeError("begin and end should be assigned")
    data = DataAPI.TradeCalGet(exchangeCD=["XSHG,XSHE"], beginDate=kwargs['begin'], endDate=kwargs['end'], isOpen=u"", field=u"", pandas="1")
    return data


if __name__ == '__main__':
    with open('token.txt', 'rt', encoding='utf-8') as f:
        token = f.read().strip()
    client = Client(token=token)
    # indexID = get_data("SecID_IDX_info")['secID'].to_list()

    # rolling_save(HKshszHold, 'HKshszHold', 20200103, 20200430, freq='q', monthlyStack=True,
    #              subPath="{}/HKshszHold".format(dataBase_root_path), ticker=stockNumList, reWrite=True)

    # induID = get_data('IndustryID_info')
    # induID_Citic = [int(i) for i in induID[induID['industryVersionCD'] == "010317"]['industryID'].to_list()]
    # induID_Sw = [int(i) for i in induID[induID['industryVersionCD'] == "010321"]['industryID'].to_list()]

    # 补数据
    for tableName in ["SecHalt"]:
        print(tableName)
        for year in range(2015, 2024):
            begin = "{}0101".format(year)
            end = "{}1231".format(year)
            rolling_save(eval(tableName), tableName, begin, end, freq='q', monthlyStack=False,
                         subPath="{}/{}".format(dataBase_root_path, tableName),
                         ticker=stockNumList, reWrite=True)
