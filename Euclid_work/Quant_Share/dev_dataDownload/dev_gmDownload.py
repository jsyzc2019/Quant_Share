import pandas as pd
from gm.api import *
from Euclid_work.Quant_Share import stock_info
from tqdm import tqdm
from Euclid_work.Quant_Share import save_data_h5, dataBase_root_path_gmStockFactor, format_date
import os


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


def save_gm_data_Y(df, date_column_name, tableName, reWrite=False):
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        df1 = df1.drop(['year'], axis=1)
        save_data_h5(df1, name='{}_Y{}'.format(tableName, yeari),
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

    fundamentals_income_info = pd.read_excel(r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_income_info.xlsx')
    fundamentals_income_fields = ",".join(fundamentals_income_info['字段名'].to_list()[:-3])  # 最后三个字段没有
    data = fundamentals_income(begin='20150101', end='20231231', symbol=symbolList,
                                 fundamentals_income_fields=fundamentals_income_fields)

    save_gm_data_Y(data, 'pub_date', 'fundamentals_income', reWrite=True)
