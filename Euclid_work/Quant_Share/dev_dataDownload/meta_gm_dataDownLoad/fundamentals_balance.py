"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/14 20:42
# @Author  : Euclid-Jie
# @File    : fundamentals_balance.py
"""
from .base_package import *
from .save_gm_data_Y import save_gm_data_Y


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
    with tqdm(symbol) as t:
        for symbol_i in t:
            t.set_description(("begin:{} -- end:{}".format(begin, end)))
            tmpData = stk_get_fundamentals_balance(symbol=symbol_i, rpt_type=None, data_type=None,
                                                   start_date=begin, end_date=end,
                                                   fields=fundamentals_balance_fields, df=True)
            t.set_postfix({"状态": "已成功获取{}条数据".format(len(tmpData))})
            outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData


def fundamentals_balance_update(upDateBegin, endDate='20231231'):
    fundamentals_balance_info = pd.read_excel(os.path.join(dev_files_dir, 'fundamentals_balance_info.xlsx'))
    fundamentals_balance_fields = ",".join(fundamentals_balance_info['字段名'].to_list())
    data = fundamentals_balance(begin=upDateBegin, end=endDate, symbol=symbolList,
                                fundamentals_balance_fields=fundamentals_balance_fields)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_gm_data_Y(data, 'pub_date', 'fundamentals_balance', reWrite=True)
