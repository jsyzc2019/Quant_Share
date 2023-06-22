"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/15 9:45
# @Author  : Euclid-Jie
# @File    : trading_derivative_indicator.py
"""
from .base_package import *
from ...Utils import save_gm_data_Q


def trading_derivative_indicator(begin, end, **kwargs):
    if 'trading_derivative_indicator_fields' not in kwargs.keys():
        raise AttributeError('trading derivative indicator fields should in kwargs!')

    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    symbol = kwargs['symbol']
    trading_derivative_indicator_fields = kwargs['trading_derivative_indicator_fields']
    outData = pd.DataFrame()
    with tqdm(patList(symbol, 8)) as t:
        t.set_description("begin:{} -- end:{}".format(begin, end))
        for patSymbol in t:
            try:
                tmpData = get_fundamentals(table='trading_derivative_indicator', symbols=patSymbol, limit=1000,
                                           start_date=begin, end_date=end, fields=trading_derivative_indicator_fields, df=True)
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


def trading_derivative_indicator_update(upDateBegin, endDate='20231231'):
    trading_derivative_indicator_info = pd.read_excel(os.path.join(dev_files_dir, 'trading_derivative_indicator.xlsx'))
    trading_derivative_indicator_fields = trading_derivative_indicator_info['列名'].to_list()
    data = trading_derivative_indicator(begin=upDateBegin, end=endDate, symbol=symbolList,
                                        trading_derivative_indicator_fields=trading_derivative_indicator_fields)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_gm_data_Q(data, 'pub_date', 'trading_derivative_indicator', reWrite=True, dataBase_root_path=dataBase_root_path_gmStockFactor)
