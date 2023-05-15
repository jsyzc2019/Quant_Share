"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/14 20:42
# @Author  : Euclid-Jie
# @File    : fundamentals_balance.py
"""
from .base_package import *


def fundamentals_balance(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    if 'fundamentals_balance_fields' not in kwargs.keys():
        raise AttributeError('fundamentals balance fields should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    # arg prepare
    symbol = kwargs['symbol']
    fundamentals_balance_fields = kwargs['fundamentals_balance_fields']

    outData = pd.DataFrame()
    for symbol_i in tqdm(symbol):
        tmpData = stk_get_fundamentals_balance(symbol=symbol_i, rpt_type=None, data_type=None,
                                               start_date=begin, end_date=end,
                                               fields=fundamentals_balance_fields, df=True)
        outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData
