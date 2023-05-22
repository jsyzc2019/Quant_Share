"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/15 9:46
# @Author  : Euclid-Jie
# @File    : balance_sheet.py
"""
from .base_package import *


def balance_sheet(begin, end, **kwargs):
    if 'balance_sheet_fields' not in kwargs.keys():
        raise AttributeError('balance sheet fields should in kwargs!')

    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    symbol = kwargs['symbol']
    balance_sheet_fields = kwargs['balance_sheet_fields']
    outData = pd.DataFrame()
    with tqdm(patList(symbol, 30)) as t:
        t.set_description("begin:{} -- end:{}".format(begin, end))
        for patSymbol in t:
            try:
                tmpData = get_fundamentals(table='balance_sheet', symbols=patSymbol, limit=1000,
                                           start_date=begin, end_date=end, fields=balance_sheet_fields, df=True)
                t.set_postfix({"状态": "已成功获取{}条数据".format(len(tmpData))})  # 进度条右边显示信息
                errors_num = 0

            except GmError:
                errors_num += 1
                if errors_num > 5:
                    raise RuntimeError("重试五次后，仍旧GmError")
                time.sleep(60)
                t.set_postfix({"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)})

            outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData


if __name__ == '__main__':
    balance_sheet_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\balance_sheet.xlsx')
    balance_sheet_fields = balance_sheet_info['列名'].to_list()
    data = balance_sheet(begin='20150101', end='20231231', symbol=symbolList,
                         balance_sheet_fields=balance_sheet_fields)
    save_gm_data_Y(data, 'pub_date', 'balance_sheet', reWrite=True)
