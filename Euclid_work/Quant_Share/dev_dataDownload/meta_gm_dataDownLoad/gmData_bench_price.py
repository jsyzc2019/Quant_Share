"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 16:11
# @Author  : Euclid-Jie
# @File    : gmData_bench_price.py
"""
from .base_package import *


def gmData_bench_price(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    outData = pd.DataFrame()
    errors_num = 0
    update_exit = 0
    with tqdm(kwargs['symbol']) as t:
        for symbol_i in t:
            try:
                data = get_history_symbol(symbol_i, start_date=begin, end_date=end, df=True)
                _len = len(data)
                t.set_postfix({"状态": "已写入{}的{}条数据".format(symbol_i, _len)})  # 进度条右边显示信息
                errors_num = 0

                if _len > 0:
                    update_exit = 0
                    outData = pd.concat([outData, data], axis=0, ignore_index=True)
                else:
                    update_exit += 1
                if update_exit >= update_exit_limit:
                    print("no data return, exit update")
                    break

            except GmError:
                errors_num += 1
                if errors_num > 5:
                    raise RuntimeError("重试五次后，仍旧GmError")
                time.sleep(60)
                t.set_postfix({"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)})

    return outData


def gmData_bench_price_update(upDateBegin, endDate='20231231'):
    bench_symbol_list = list(set(bench_info['symbol']))
    data = gmData_bench_price(begin=upDateBegin, end=endDate, symbol=bench_symbol_list)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Y(data, 'trade_date', 'gmData_bench_price', reWrite=True, _dataBase_root_path=dataBase_root_path_gmStockFactor)
