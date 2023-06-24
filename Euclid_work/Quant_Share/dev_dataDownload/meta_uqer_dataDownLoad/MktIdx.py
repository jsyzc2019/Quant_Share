"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 21:30
# @Author  : Euclid-Jie
# @File    : MktIdx.py
"""
from .base_package import *
from .rolling_save import rolling_save


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


def MktIdx_update(upDateBegin, endDate='20231231'):
    indexID = get_data("SecID_IDX_info")['secID'].to_list()
    rolling_save(MktIdx, 'MktIdx', upDateBegin, endDate, freq='q', subPath="{}/MktIdx".format(dataBase_root_path),
                 reWrite=True, monthlyStack=False, indexID=indexID)
