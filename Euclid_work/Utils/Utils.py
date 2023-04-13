# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 18:47
# @Author  : Euclid-Jie
# @File    : Utils.py

import datetime
import os
import os.path
import pickle

import numpy as np
import pandas as pd
from gm.api import *
from uqer import DataAPI

__all__ = ['readPkl', 'savePkl', 'save_data_h5',  # files operation
           'get_tradeDate', 'format_date', 'format_stockCode', 'reindex', 'data2score', 'info_lag',
           'format_futures',
           # Consts
           'stock_info', 'stockList', 'stockNumList', 'bench_info', 'tradeDate_info', 'tradeDateList', 'quarter_begin', 'quarter_end',
           'futures_list']


def format_stockCode(numCode):
    """
    Standardize the stock code to wind format
    :param numCode: '000001', '000001.XSHE' etc.
    :return: '000001.SZ'
    """
    if isinstance(numCode, str):
        if numCode[-2:] in ['BJ', 'SZ', 'SH']:
            windCode = numCode
            return windCode
        elif "." in numCode or numCode.isdigit():
            if numCode[:6].isdigit():
                num = numCode[:6]
            elif numCode[-6:].isdigit():
                num = numCode[-6:]
            else:
                return np.nan
            numCode = int(num)
        else:
            return np.nan
    tag = numCode // 100000
    if tag in [4, 8]:
        tail = 'BJ'
    elif numCode < 500000:
        tail = 'SZ'
    else:
        tail = 'SH'
    windCode = "{:06.0f}.{}".format(numCode, tail)
    return windCode


def format_futures(file_name):
    if 'qe' in file_name:
        out = file_name.split("_")[0].upper()
    else:
        out = file_name.split('.')[1]
        if "_" in out:
            out = out.split('_')[0].upper()

    if out in futures_list:
        return out
    else:
        raise KeyError('{} is not format futures!'.format(file_name))


# consts
stock_info = pd.read_hdf('dataFile/stock_info.h5')
stockList = [format_stockCode(code) for code in stock_info['symbol']]  # 000001.SZ
stockNumList = list(set(stock_info.sec_id))  # 000001
futures_list = ['AG', 'AL', 'AU', 'A', 'BB', 'BU', 'B', 'CF', 'CS', 'CU', 'C', 'FB', 'FG', 'HC', 'IC', 'IF', 'IH', 'I', 'JD', 'JM', 'JR', 'J', 'LR', 'L', 'MA', 'M', 'NI', 'OI',
                'PB', 'PM', 'PP', 'P', 'RB', 'RI', 'RM', 'RS', 'RU', 'SF', 'SM', 'SN', 'SR', 'TA', 'TF', 'T', 'V', 'WH', 'Y', 'ZC', 'ZN', 'PG',
                'EB', 'AP', 'LU', 'SA', 'TS', 'CY', 'IM', 'PF', 'PK', 'CJ', 'UR', 'NR', 'SS', 'FU', 'EG', 'LH', 'SP', 'RR', 'SC', 'WR', 'BC']

bench_info = pd.read_hdf('dataFile/bench_info.h5')
tradeDate_info = pd.read_hdf("dataFile/tradeDate_info.h5")
tradeDateList = list(set(tradeDate_info['tradeDate']))[1:]
quarter_begin = ['0101', '0401', '0731', '1001']
quarter_end = ['0331', '0630', '0930', '1231']


# TODO 股票池 行业中心化 标准回测 groupBy加速


def readPkl(fpath):
    with open(fpath, "rb") as f:
        res = pickle.load(f)
    return res


def savePkl(obj, fpath):
    with open(fpath, "wb") as f:
        pickle.dump(obj, f)
    return


def data2score(data, neg=False, ascending=True, axis=1):
    """
    use rank as score
    :param data:
    :param neg: if Ture, score span [-1, 1], default FALSE
    :param ascending:
    :param axis:
    :return:
    """
    score = data.rank(axis=axis, ascending=ascending, pct=True)
    if neg:
        score = score * 2 - 1
    return pd.DataFrame(data=score, columns=data.columns, index=data.index)


def reindex(data):
    """
    Convert wide table to standard format, with index as pd.dt and columns as wind code
    :param data:
    :return:
    """
    if not isinstance(data, pd.DataFrame):
        raise TypeError("data should be pd.DataFrame!")
    if not isinstance(data.index[0], datetime.datetime):
        data.index = [format_date(x) for x in data.index]
    data.columns = [format_stockCode(x) for x in data.columns]
    if np.NAN in data.columns:
        data.drop(columns=np.NAN, inplace=True)

    new_index = [x for x in pd.date_range(data.index.min(), data.index.max(), freq='D') if x in tradeDateList]
    new_columns = stockList
    return data.reindex(index=new_index, columns=new_columns, fill_value=np.NAN)


def info_lag(data, n_lag):
    """
    Delay the time corresponding to the data by n trading days
    :param data:
    :param n_lag:
    :return:
    """
    new_index = [get_tradeDate(x, n_lag)['tradeDate_fore'] for x in data.index]
    out = data.copy()
    out.index = new_index
    return out


def save_data_h5(toSaveData, name, subPath='dataFile', reWrite=False):
    """
    Store the pd.Data Frame data as a file in .h5 format
    :param toSaveData:
    :param name:
    :param subPath: The path to store the file will be 'cwd/subPath/name.h5', default 'dataFile'
    :param reWrite: if Ture, will rewrite file, default False
    :return:
    """
    # format confirm
    if name[-3] != '.h5':
        name = name + '.h5'
    # save path
    if subPath:
        if not os.path.exists(subPath):
            os.mkdir(subPath)
        fullPath = os.path.join(subPath, name)
    else:
        fullPath = name
    if isinstance(toSaveData, pd.DataFrame):
        if reWrite:
            toSaveData.to_hdf(fullPath, 'a', 'w')
        else:
            if os.path.exists(fullPath):
                raise FileExistsError("{} has Existed!".format(fullPath))
            toSaveData.to_hdf(fullPath, 'a', 'w')
    else:
        raise TypeError("only save pd.DataFrame format!")
        # pd.DataFrame(toSaveData).to_hdf(name,'a','w')


def get_tradeDate(date, n=0):
    """
    Returns the date related to date based on the setting of n
    if n = 0, will return the future the nearest trade date, if date is trade date, will return itself
    if n = -1, will return the backward the nearest trade date, if date is trade date, will return itself
    else will returns information from the delay n days (calendar, tradeDate_fore and tradeDate_back）
    :param date:
    :param n: default 0
    :return:
    """
    date = format_date(date)
    if n == 0:
        return tradeDate_info.loc[date]['tradeDate_fore']
    elif n == -1:
        return tradeDate_info.loc[date]['tradeDate_back']
    else:
        index = tradeDate_info.index.to_list().index(date) + n
        return tradeDate_info.iloc[index]


def format_date(date):
    if isinstance(date, datetime.datetime):
        return date
    elif isinstance(date, int):
        date = pd.to_datetime(date, format='%Y%m%d')
        return date
    elif isinstance(date, str):
        date = pd.to_datetime(date)
        return date
    else:
        raise TypeError("date should be str or int!")


# 以下函数仅为参照，不能也不应被调用
def get_tradeDate_info(endDate='2023-12-31'):
    """
    use gm get
    calendar date \
    trade date \
    trade date fore(the nearest trade date in future) \
    trade date back(the nearest trade date in history)
    """
    endDate = format_date(endDate)
    tradeDate_info = pd.DataFrame(get_trading_dates('SHSE', start_date='2015-01-01', end_date=endDate))
    tradeDate_info.columns = ['tradeDate']
    tradeDate_info['tradeDate'] = pd.to_datetime(tradeDate_info['tradeDate'])

    date_info = pd.date_range(start='2015-01-01', end='2023-12-31', freq='d').to_frame(name='calendar')
    tradeDate_info = date_info.merge(tradeDate_info, left_on='calendar', right_on='tradeDate', how='left')

    tradeDate_info['tradeDate_fore'] = tradeDate_info['tradeDate'].fillna(method='bfill')
    tradeDate_info['tradeDate_back'] = tradeDate_info['tradeDate'].fillna(method='ffill')

    tradeDate_info.index = tradeDate_info['calendar']
    return tradeDate_info


# 设计一个季度存储的数据
# pd.tseries.offsets.MonthEnd(1)
def quarter_download_save(year):
    for quarter in range(0, 4):
        quarter_begin_day = str(year) + quarter_begin[quarter]
        quarter_end_day = str(year) + quarter_end[quarter]
        data = DataAPI.HKshszHoldGet(secID=u"", ticker=u"", tradeCD=['1', '2'],
                                     beginDate=quarter_begin_day,
                                     endDate=quarter_end_day, field=u"", pandas="1")
        save_data_h5(data, name='HKshszHold_Y{}_Q{}'.format(year, quarter + 1), subPath="dataFile/HKshszHold")


def year_download_save():
    for year in range(2015, 2024):
        data = get_price_vol(year)
        save_data_h5(data, 'bench_price_Y{}'.format(year), subPath='dataFile/bench_price')


def get_price_vol(year):
    year_begin = format_date(str(year) + '0101')
    year_end = format_date(str(year) + '1231')
    return get_history_instruments(symbols=stock_info['symbol'].to_list(),
                                   start_date=get_tradeDate(year_begin).strftime('%Y-%m-%d'),
                                   end_date=get_tradeDate(year_end, -1).strftime('%Y-%m-%d'), df=True)
