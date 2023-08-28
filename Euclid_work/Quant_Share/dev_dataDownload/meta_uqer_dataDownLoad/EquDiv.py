"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:27
# @Author  : Euclid-Jie
# @File    : MktEqud.py
"""
from .base_package import *
from .rolling_save import rolling_save


def EquDiv(begin, end, **kwargs):
    """
    股票分红信息
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.EquDiv(
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


def EquDiv_update(upDateBegin, endDate="20231231"):
    rolling_save(
        EquDiv,
        "EquDiv",
        upDateBegin,
        endDate,
        freq="q",
        subPath="{}/EquDiv".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=stockNumList,
    )
