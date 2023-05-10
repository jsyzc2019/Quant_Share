import time

import pandas as pd
from gm.api import *
from Euclid_work.Quant_Share import stock_info
from tqdm import tqdm
from Euclid_work.Quant_Share import save_data_h5, dataBase_root_path_gmStockFactor, format_date, patList
import os
from Test.AutoEmail import AutoEmail


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


def fundamentals_cashflow(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    if 'fundamentals_cashflow_fields' not in kwargs.keys():
        raise AttributeError('fundamentals cashflow fields should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    # arg prepare
    symbol = kwargs['symbol']
    fundamentals_cashflow_fields = kwargs['fundamentals_cashflow_fields']

    outData = pd.DataFrame()
    for symbol_i in tqdm(symbol):
        tmpData = stk_get_fundamentals_cashflow(symbol=symbol_i, rpt_type=None, data_type=None,
                                                start_date=begin, end_date=end,
                                                fields=fundamentals_cashflow_fields, df=True)
        outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData


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


def share_change(begin, end, **kwargs):
    if 'symbol' not in kwargs.keys():
        raise AttributeError('symbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    symbol = kwargs['symbol']

    outData = pd.DataFrame()
    for symbol_i in tqdm(symbol):
        tmpData = stk_get_share_change(symbol=symbol_i, start_date=begin, end_date=end)
        outData = pd.concat([outData, tmpData], ignore_index=True)
    return outData


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
    with tqdm(patList(symbol, 4)) as t:
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


def save_gm_data_Y(df, date_column_name, tableName, reWrite=False):
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        df1 = df1.drop(['year'], axis=1)
        save_data_h5(df1, name='{}_Y{}'.format(tableName, yeari),
                     subPath="{}/{}".format(dataBase_root_path_gmStockFactor, tableName), reWrite=reWrite)


def save_gm_dataQ(df, date_column_name, tableName, reWrite=False):
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    df["quarter"] = df[date_column_name].apply(lambda x: format_date(x).quarter)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        for quarteri in range(df1["quarter"].min(), df1["quarter"].max() + 1):
            df2 = df1[df1["quarter"] == quarteri]
            df2 = df2.drop(columns=['year', 'quarter'], axis=1)
            save_data_h5(df2, name='{}_Y{}_Q{}'.format(tableName, yeari, quarteri),
                         subPath="{}/{}".format(dataBase_root_path_gmStockFactor, tableName), reWrite=reWrite)


if __name__ == '__main__':
    set_token('cac6f11ecf01f9539af72142faf5c3066cb1915b')
    symbolList = list(stock_info.symbol.unique())

    # fundamentals_balance_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_balance_info.xlsx')
    # fundamentals_balance_fields = ",".join(fundamentals_balance_info['字段名'].to_list())
    # data = fundamentals_balance(begin='20150101', end='20231231', symbol=symbolList,
    #                             fundamentals_balance_fields=fundamentals_balance_fields)

    # data = share_change(begin='20150101', end='20231231', symbol=symbolList)

    # fundamentals_cashflow_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_cashflow_info.xlsx')
    # fundamentals_cashflow_fields = ",".join(fundamentals_cashflow_info['字段名'].to_list()[:-3])  # 最后三个字段没有
    # data = fundamentals_cashflow(begin='20150101', end='20231231', symbol=symbolList,
    #                              fundamentals_cashflow_fields=fundamentals_cashflow_fields)

    # fundamentals_income_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_income_info.xlsx')
    # fundamentals_income_fields = ",".join(fundamentals_income_info['字段名'].to_list()[:-3])  # 最后三个字段没有
    # data = fundamentals_income(begin='20150101', end='20231231', symbol=symbolList,
    #                            fundamentals_income_fields=fundamentals_income_fields)

    trading_derivative_indicator_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\trading_derivative_indicator.xlsx')
    trading_derivative_indicator_fields = trading_derivative_indicator_info['列名'].to_list()
    for year in range(2015, 2024):
        begin = "{}0101".format(year)
        end = "{}1231".format(year)
        data = trading_derivative_indicator(begin=begin, end=end, symbol=symbolList,
                                            trading_derivative_indicator_fields=trading_derivative_indicator_fields)

        save_gm_dataQ(data, 'pub_date', 'trading_derivative_indicator', reWrite=True)
        AutoEmail('trading_derivative_indicator for {} has done'.format(year))