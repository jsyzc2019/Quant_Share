"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 16:11
# @Author  : Euclid-Jie
# @File    : gmData_bench_price.py
"""
from .base_package import *
from ...Utils import save_gm_data_Y


def gmData_bench_price(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    outData = pd.DataFrame()
    with tqdm(kwargs['symbol']) as t:
        for symbol_i in t:
            try:
                data = get_history_symbol(symbol_i, start_date=begin, end_date=end, df=True)
                t.set_postfix({"状态": "已写入{}数据".format(symbol_i)})  # 进度条右边显示信息
            except GmError:
                t.set_postfix({"状态": "GmError{}".format(GmError)})  # 进度条右边显示信息
            outData = pd.concat([outData, data], axis=0, ignore_index=True)
    return outData


def gmData_bench_price_update(upDateBegin, endDate='20231231'):
    bench_symbol_list = list(set(bench_info['symbol']))
    data = gmData_bench_price(begin=upDateBegin, end=endDate, symbol=bench_symbol_list)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_gm_data_Y(data, 'trade_date', 'gmData_bench_price', reWrite=True, dataBase_root_path=dataBase_root_path_gmStockFactor)
