"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 22:22
# @Author  : Euclid-Jie
# @File    : FdmtIndiRtnPit.py
"""
from .base_package import *
from .rolling_save import rolling_save


def FdmtIndiRtnPit(begin, end, **kwargs):
    """
    指数日行情
    :param begin:
    :param end:
    :param kwargs: indexID = get_data("SecID_IDX_info")['secID'].to_list()
    :return:
    """
    if 'ticker' not in kwargs.keys():
        raise AttributeError('ticker should in kwargs!')
    data = DataAPI.FdmtIndiRtnPitGet(ticker=kwargs['ticker'], secID="", beginDate=begin, endDate=end, beginYear=u"", endYear=u"", reportType=u"",
                                     publishDateEnd=u"", publishDateBegin=u"", field=u"", pandas="1",
                                     pat_len=100)
    return data


def FdmtIndiRtnPit_update(upDateBegin, endDate='20231231'):
    rolling_save(FdmtIndiRtnPit, 'FdmtIndiRtnPit', upDateBegin, endDate, freq='q', subPath="{}/FdmtIndiRtnPit".format(dataBase_root_path),
                 reWrite=True, monthlyStack=False, ticker=stockNumList)
