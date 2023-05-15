"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/14 20:43
# @Author  : Euclid-Jie
# @File    : fundamentals_income.py
"""
from base_package import *


def fundamentals_income(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    if 'fundamentals_income_fields' not in kwargs.keys():
        raise AttributeError('fundamentals income fields should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    # arg prepare
    symbol = kwargs['symbol']
    fundamentals_income_fields = kwargs['fundamentals_income_fields']

    outData = pd.DataFrame()
    for symbol_i in tqdm(symbol):
        tmpData = stk_get_fundamentals_income(symbol=symbol_i, rpt_type=None, data_type=None,
                                              start_date=begin, end_date=end,
                                              fields=fundamentals_income_fields, df=True)
        outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData


if __name__ == '__main__':
    fundamentals_income_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_income_info.xlsx')
    fundamentals_income_fields = ",".join(fundamentals_income_info['字段名'].to_list()[:-3])  # 最后三个字段没有
    data = fundamentals_income(begin='20150101', end='20231231', symbol=symbolList,
                               fundamentals_income_fields=fundamentals_income_fields)
    save_gm_data_Y(data, 'pub_date', 'fundamentals_income', reWrite=True)
