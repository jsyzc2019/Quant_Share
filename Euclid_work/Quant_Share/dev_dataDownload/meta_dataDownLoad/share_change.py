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
    for symbol_i in tqdm(symbol):
        tmpData = stk_get_share_change(symbol=symbol_i, start_date=begin, end_date=end)
        outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData
