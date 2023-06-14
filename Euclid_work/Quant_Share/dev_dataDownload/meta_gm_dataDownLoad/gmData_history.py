"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:54
# @Author  : Euclid-Jie
# @File    : gmData_history.py
"""
from .base_package import *
from .save_gm_data_Q import *


def gmData_history(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    outData = pd.DataFrame()
    with tqdm(kwargs['symbol']) as t:
        for symbol_i in t:
            try:
                data = history(symbol_i, frequency='1d', start_time=begin, end_time=end, df=True)
                t.set_postfix({"状态": "已写入{}数据".format(symbol_i)})  # 进度条右边显示信息
            except GmError:
                t.set_postfix({"状态": "GmError:{}".format(GmError)})  # 进度条右边显示信息
            outData = pd.concat([outData, data], axis=0, ignore_index=True)
    return outData


def gmData_history_update(upDateBegin, endDate='20231231'):
    data = gmData_history(begin=upDateBegin, end=endDate, symbol=symbolList)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_gm_data_Q(data, 'bob', 'gmData_history', reWrite=True)
