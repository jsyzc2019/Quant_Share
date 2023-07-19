"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 16:12
# @Author  : Euclid-Jie
# @File    : MktLimit.py
"""
from .base_package import *
from .rolling_save import rolling_save


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


def MktLimit_update(upDateBegin, endDate="20231231"):
    rolling_save(
        MktLimit,
        "MktLimit",
        upDateBegin,
        endDate,
        freq="q",
        subPath="{}/MktLimit".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=stockNumList,
    )
