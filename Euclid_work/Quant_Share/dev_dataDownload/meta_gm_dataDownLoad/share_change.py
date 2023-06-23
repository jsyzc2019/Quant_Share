"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/14 20:44
# @Author  : Euclid-Jie
# @File    : share_change.py
"""
from .base_package import *


def share_change(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    symbol = kwargs['symbol']

    outData = pd.DataFrame()
    with tqdm(symbol) as t:
        t.set_description(("begin:{} -- end:{}".format(begin, end)))
        for symbol_i in t:
            tmpData = stk_get_share_change(symbol=symbol_i, start_date=begin, end_date=end)
            outData = pd.concat([outData, tmpData], ignore_index=True)
            t.set_postfix({"状态": "已成功获取{}条数据".format(len(tmpData))})
    return outData


def share_change_update(upDateBegin, endDate='20231231'):
    data = share_change(begin=upDateBegin, end=endDate, symbol=symbolList)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Y(data, 'pub_date', 'share_change', reWrite=True, _dataBase_root_path=dataBase_root_path_gmStockFactor)
