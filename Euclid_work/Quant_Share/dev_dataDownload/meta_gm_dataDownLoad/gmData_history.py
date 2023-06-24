"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:54
# @Author  : Euclid-Jie
# @File    : gmData_history.py
"""
from .base_package import *


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


def gmData_history_update(upDateBegin, endDate='20231231'):
    data = gmData_history(begin=upDateBegin, end=endDate, symbol=symbolList)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Q(data, 'bob', 'gmData_history', reWrite=True, _dataBase_root_path=dataBase_root_path_gmStockFactor)
