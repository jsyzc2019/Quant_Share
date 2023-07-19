"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 23:10
# @Author  : Euclid-Jie
# @File    : FdmtIndiPSPit.py
"""
from .base_package import *
from .rolling_save import rolling_save


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
        pat_len=100,
    )
    return data


def FdmtIndiPSPit_update(upDateBegin, endDate="20231231"):
    rolling_save(
        FdmtIndiPSPit,
        "FdmtIndiPSPit",
        upDateBegin,
        endDate,
        freq="q",
        subPath="{}/FdmtIndiPSPit".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=stockNumList,
    )
