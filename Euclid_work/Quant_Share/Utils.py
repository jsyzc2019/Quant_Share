# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 18:47
# @Author  : Euclid-Jie
# @File    : Utils.py

import datetime
import json
import os
import os.path
import pickle
from tqdm import tqdm
from uqer import DataAPI
import numpy as np
import pandas as pd
from gm.api import *
from functools import reduce, wraps

dataBase_root_path = r'E:\Share\Stk_Data\dataFile'
dataBase_root_path_future = r"E:\Share\Fut_Data"
dataBase_root_path_gmStockFactor = r"E:\Share\Stk_Data\gm"
dataBase_root_path_EMdata = r"E:\Share\EMData"

# dataBase_root_path = r'D:\Share\Euclid_work\dataFile'
# dataBase_root_path_future = r"D:\Share\Fut_Data"
# dataBase_root_path_gmStockFactor = r"D:\Share\Stk_Data\gm"

__all__ = ['readPkl', 'savePkl', 'save_data_h5',  # files operation
           'get_tradeDate', 'format_date', 'format_stockCode', 'reindex', 'data2score', 'info_lag',
           'format_futures', 'printJson', 'extend_date_span', 'patList', 'is_tradeDate', 'get_tradeDates',
           # Consts
           'stock_info', 'stockList', 'stockNumList', 'bench_info', 'tradeDate_info', 'tradeDateList', 'quarter_begin', 'quarter_end',
           'futures_list', 'dataBase_root_path', 'dataBase_root_path_future', 'dataBase_root_path_gmStockFactor',
           'dataBase_root_path_EMdata',
           # decorator
           'time_decorator', 'lazyproperty']


def time_decorator(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(f'“{func.__name__}” run time: {end - start}.')
        return result
    return timer

class lazyproperty:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value

def patList(InList: list, pat: int):
    return [InList[i: i + pat] for i in range(0, len(InList), pat)]


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
stock_info = pd.read_hdf('{}/stock_info.h5'.format(dataBase_root_path))
stockList = [format_stockCode(code) for code in stock_info['symbol']]  # 000001.SZ
stockNumList = list(set(stock_info.sec_id))  # 000001
futures_list = ['AG', 'AL', 'AU', 'A', 'BB', 'BU', 'B', 'CF', 'CS', 'CU', 'C', 'FB', 'FG', 'HC', 'IC', 'IF', 'IH', 'I', 'JD', 'JM', 'JR', 'J', 'LR', 'L', 'MA', 'M', 'NI', 'OI',
                'PB', 'PM', 'PP', 'P', 'RB', 'RI', 'RM', 'RS', 'RU', 'SF', 'SM', 'SN', 'SR', 'TA', 'TF', 'T', 'V', 'WH', 'Y', 'ZC', 'ZN', 'PG',
                'EB', 'AP', 'LU', 'SA', 'TS', 'CY', 'IM', 'PF', 'PK', 'CJ', 'UR', 'NR', 'SS', 'FU', 'EG', 'LH', 'SP', 'RR', 'SC', 'WR', 'BC']

bench_info = pd.read_hdf('{}/bench_info.h5'.format(dataBase_root_path))
tradeDate_info = pd.read_hdf("{}/tradeDate_info.h5".format(dataBase_root_path))
tradeDateList = tradeDate_info['tradeDate'].dropna().to_list()
quarter_begin = ['0101', '0401', '0701', '1001']
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


def reindex(data, tradeDate=True, **kwargs):
    """
    Convert wide table to standard format, with index as pd.dt and columns as wind code
    :param tradeDate: if ture, index is trade day
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
    if tradeDate:
        new_index = [x for x in pd.date_range(data.index.min(), data.index.max(), freq='D') if x in tradeDateList]
    else:
        new_index = pd.date_range(data.index.min(), data.index.max(), freq='D')
    new_columns = stockList
    # fill na
    fill_value = np.nan
    if 'fill_value' in kwargs.keys():
        fill_value = kwargs['fill_value']
    return data.reindex(index=new_index, columns=new_columns, fill_value=fill_value)


def info_lag(data, n_lag):
    """
    Delay the time corresponding to the data by n trading days
    :param data:
    :param n_lag:
    :return:
    """
    new_index = [get_tradeDate(x, n_lag) for x in data.index]
    out = data.copy()
    out.index = new_index
    return out


def save_data_h5(toSaveData, name, subPath='dataFile', reWrite=False):
    """
    Store the pd.Data Frame data as a file in .h5 format
    :param toSaveData:
    :param name:
    :param subPath: The path to store the file will be 'cwd/subPath/name.h5', default 'dataFile'
    :param reWrite: if Ture, will rewrite or update file, default False
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
            if os.path.exists(fullPath):
                existsData = pd.read_hdf(fullPath)
                allData = pd.concat([existsData, toSaveData]).drop_duplicates()
                if len(allData) > len(existsData):
                    print("{} has Updated!".format(fullPath))
                else:
                    print("{} has none Updated!".format(fullPath))
                toSaveData.to_hdf(fullPath, 'a', 'w')

        else:
            if os.path.exists(fullPath):
                raise FileExistsError("{} has Existed!".format(fullPath))
            toSaveData.to_hdf(fullPath, 'a', 'w')
    else:
        raise TypeError("only save pd.DataFrame format!")
        # pd.DataFrame(toSaveData).to_hdf(name,'a','w')


def get_tradeDate(InputDate, lag=0):
    """
    Returns the date related to date based on the setting of n
    if n = 0, will return the future the nearest trade date, if date is trade date, will return itself
    if n = -1, will return the backward the nearest trade date, if date is trade date, will return itself
    else will returns information from the delay n days (calendar, tradeDate_fore and tradeDate_back）
    :param InputDate:
    :param lag: default 0
    :return:
    """
    date = format_date(InputDate)
    if lag == 0:
        return tradeDate_info.loc[date]['tradeDate_fore']
    elif lag == -1:
        return tradeDate_info.loc[date]['tradeDate_back']
    else:
        res, index = binary_search(tradeDateList, date)
        if res:
            return tradeDateList[index + lag]
        else:
            # print("{} is not tradeDate".format(date))
            return tradeDateList[index + lag]


def get_tradeDates(begin, end=None, n: int = None):
    """
    获取指定时间段内的交易日列表
    :param begin:
    :param end:
    :param n:
    :return:
    """
    begin = format_date(begin)
    _, index_begin = binary_search(tradeDateList, begin)
    if end:
        end = format_date(end)
        res, index_end = binary_search(tradeDateList, end)
        if not res:
            index_end += 1
        return tradeDateList[index_begin:index_end+1]
    else:
        if n:
            return tradeDateList[index_begin:index_begin + n + 1]
        else:
            raise AttributeError("u should input end or n!")


def binary_search(arr: list, target):
    low = 0
    high = len(arr) - 1

    while low <= high:
        mid = (low + high) // 2

        if arr[mid] == target:
            return True, mid
        elif arr[mid] < target:
            low = mid + 1
        else:
            high = mid - 1

    return False, low


def is_tradeDate(date: int or str or datetime.datetime):
    if format_date(date) in tradeDateList:
        return True
    else:
        return False


def format_date(date):
    if isinstance(date, datetime.datetime):
        return pd.to_datetime(date.date())
    elif isinstance(date, int):
        date = pd.to_datetime(date, format='%Y%m%d')
        return date
    elif isinstance(date, str):
        date = pd.to_datetime(date)
        return pd.to_datetime(date.date())
    else:
        raise TypeError("date should be str or int!")


def printJson(dataJson):
    if not isinstance(dataJson, dict):
        raise TypeError("dataJson should be dict format")
    print(json.dumps(dataJson, ensure_ascii=False, indent=2))


def extend_date_span(begin, end, freq):
    begin = format_date(begin)
    end = format_date(end)
    if freq in ["Q", "q"]:
        if not pd.DateOffset().is_quarter_start(begin):
            begin = begin - pd.offsets.QuarterBegin(n=1, startingMonth=1)
        if not pd.offsets.DateOffset().is_quarter_end(end):
            end = end + pd.offsets.QuarterEnd(n=1)
        return begin, end

    elif freq in ["Y", "y"]:
        if not pd.offsets.DateOffset().is_year_start(begin):
            begin = begin - pd.offsets.YearBegin(n=1)
        if not pd.offsets.DateOffset().is_year_end(end):
            end = end + pd.offsets.YearEnd(n=1)
        return begin, end
    elif freq in ["M", "m"]:
        if not pd.DateOffset().is_month_start(begin):
            begin = begin - pd.offsets.MonthBegin(n=1)
        if not pd.offsets.DateOffset().is_month_end(end):
            end = end + pd.offsets.MonthEnd(n=1)
        return begin, end
    else:
        raise AttributeError("frq should be M, Q or Y!")