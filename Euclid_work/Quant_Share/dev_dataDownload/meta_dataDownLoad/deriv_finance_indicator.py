"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/15 9:46
# @Author  : Euclid-Jie
# @File    : deriv_finance_indicator.py
"""

from base_package import *


def deriv_finance_indicator(begin, end, **kwargs):
    if 'deriv_finance_indicator_fields' not in kwargs.keys():
        raise AttributeError('deriv finance indicator fields should in kwargs!')

    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    symbol = kwargs['symbol']
    deriv_finance_indicator_fields = kwargs['deriv_finance_indicator_fields']
    outData = pd.DataFrame()
    with tqdm(patList(symbol, 30)) as t:
        t.set_description("begin:{} -- end:{}".format(begin, end))
        for patSymbol in t:
            try:
                tmpData = get_fundamentals(table='deriv_finance_indicator', symbols=patSymbol, limit=1000,
                                           start_date=begin, end_date=end, fields=deriv_finance_indicator_fields, df=True)
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
    deriv_finance_indicator_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\deriv_finance_indicator.xlsx')
    deriv_finance_indicator_fields = deriv_finance_indicator_info['列名'].to_list()
    data = deriv_finance_indicator(begin='20150101', end='20231231', symbol=symbolList,
                                   deriv_finance_indicator_fields=deriv_finance_indicator_fields)
    save_gm_data_Y(data, 'pub_date', 'deriv_finance_indicator', reWrite=True)
