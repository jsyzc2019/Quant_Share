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
import numpy as np
import pandas as pd
from functools import reduce, wraps
from time import strptime
from datetime import datetime as dt

dataBase_root_path = r'E:\Share\Stk_Data\dataFile'
dataBase_root_path_future = r"E:\Share\Fut_Data"
dataBase_root_path_gmStockFactor = r"E:\Share\Stk_Data\gm"
dataBase_root_path_EM_data = r"E:\Share\EM_Data"
dataBase_root_path_JointQuant_Factor = r"E:\Share\JointQuant_Factor"

# dataBase_root_path = r'D:\Share\Euclid_work\dataFile'
# dataBase_root_path_future = r"D:\Share\Fut_Data"
# dataBase_root_path_gmStockFactor = r"D:\Share\Stk_Data\gm"

__all__ = ['readPkl', 'savePkl', 'save_data_h5',  # files operation
           'get_tradeDate', 'format_date', 'format_stockCode', 'reindex', 'data2score', 'info_lag',
           'format_futures', 'printJson', 'extend_date_span', 'patList', 'is_tradeDate', 'get_tradeDates',
           'isdate', 'binary_search', 'winsorize_med', 'standardlize', 'save_data_Y', 'save_data_Q',
           # Consts
           'stock_info', 'stockList', 'stockNumList', 'bench_info', 'tradeDate_info', 'tradeDateList', 'quarter_begin',
           'quarter_end',
           'futures_list', 'dataBase_root_path', 'dataBase_root_path_future', 'dataBase_root_path_gmStockFactor',
           'dataBase_root_path_EM_data', 'dataBase_root_path_JointQuant_Factor',
           # decorator
           'time_decorator', 'lazyproperty']


def time_decorator(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = dt.now()
        result = func(*args, **kwargs)
        end = dt.now()
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
futures_list = ['AG', 'AL', 'AU', 'A', 'BB', 'BU', 'B', 'CF', 'CS', 'CU', 'C', 'FB', 'FG', 'HC', 'IC', 'IF', 'IH', 'I',
                'JD', 'JM', 'JR', 'J', 'LR', 'L', 'MA', 'M', 'NI', 'OI',
                'PB', 'PM', 'PP', 'P', 'RB', 'RI', 'RM', 'RS', 'RU', 'SF', 'SM', 'SN', 'SR', 'TA', 'TF', 'T', 'V', 'WH',
                'Y', 'ZC', 'ZN', 'PG',
                'EB', 'AP', 'LU', 'SA', 'TS', 'CY', 'IM', 'PF', 'PK', 'CJ', 'UR', 'NR', 'SS', 'FU', 'EG', 'LH', 'SP',
                'RR', 'SC', 'WR', 'BC']

bench_info = pd.read_hdf('{}/bench_info.h5'.format(dataBase_root_path))  # gm's bench info
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


def winsorize_med(data: pd.Series, scale=1, inclusive: bool = True, inf2nan: bool = True):
    s = data.copy()
    if inf2nan:
        s[np.isinf(s)] = np.nan
    med = np.nanmedian(s)
    distance = np.nanmedian(np.abs(s - med))
    up = med + scale * distance
    down = med - scale * distance
    if inclusive:
        s = np.clip(s, down, up)
    else:
        s[s > up] = np.nan
        s[s < down] = np.nan
    return s


def standardlize(data: pd.Series, inf2nan=True):
    s = data.copy()
    if inf2nan:
        s[np.isinf(s)] = np.nan
        mean = np.nanmean(s)
        std = np.nanstd(s, ddof=1)
    else:
        s1 = s[~np.isinf(s)]
        mean = np.nanmean(s1)
        std = np.nanstd(s1, ddof=1)
    s = (s - mean) / std
    _range = np.nanmax(s) - np.nanmin(s)
    return (s - np.nanmin(s)) / _range


def reindex(data, tradeDate=True, **kwargs):
    """
    Convert wide table to standard format, with index as pd.dt and columns as wind code
    :param tradeDate: if ture, index is trade day
    :param data:
    :param kwargs: begin=min(data.index), end=max(data.index), fill_value=np.nan
    :return:
    """
    if not isinstance(data, pd.DataFrame):
        raise TypeError("data should be pd.DataFrame!")
    if not isinstance(data.index[0], datetime.datetime):
        data.index = [format_date(x) for x in data.index]
    data.columns = [format_stockCode(x) for x in data.columns]
    if np.NAN in data.columns:
        data.drop(columns=np.NAN, inplace=True)
    begin = kwargs.get('begin', data.index.min())
    end = kwargs.get('end', data.index.max())
    if tradeDate:
        # TODO 使用二分法优化速度
        new_index = [x for x in pd.date_range(begin, end, freq='D') if x in tradeDateList]
    else:
        new_index = pd.date_range(begin, end, freq='D')
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
                print("{} has been created!".format(fullPath))
                toSaveData.to_hdf(fullPath, 'a', 'w')

        else:
            if os.path.exists(fullPath):
                raise FileExistsError("{} has Existed!".format(fullPath))
            toSaveData.to_hdf(fullPath, 'a', 'w')
    else:
        raise TypeError("only save pd.DataFrame format!")
        # pd.DataFrame(toSaveData).to_hdf(name,'a','w')

def save_data_Y(df, date_column_name, tableName, dataBase_root_path, reWrite=False):
    """
    对df进行拆分, 进一步存储
    """
    if len(df) == 0:
        raise ValueError("data for save is null!")
    print("数据将存储在: {}/{}".format(dataBase_root_path, tableName))
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        df1 = df1.drop(['year'], axis=1)
        save_data_h5(df1, name='{}_Y{}'.format(tableName, yeari),
                     subPath="{}/{}".format(dataBase_root_path, tableName), reWrite=reWrite)

def save_data_Q(df, date_column_name, tableName, dataBase_root_path, reWrite=False):
    """
    对df进行拆分, 进一步存储
    """
    if len(df) == 0:
        raise ValueError("data for save is null!")
    print("数据将存储在: {}/{}".format(dataBase_root_path, tableName))
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    df["quarter"] = df[date_column_name].apply(lambda x: format_date(x).quarter)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        for quarteri in range(df1["quarter"].min(), df1["quarter"].max() + 1):
            df2 = df1[df1["quarter"] == quarteri]
            df2 = df2.drop(columns=['year', 'quarter'], axis=1)
            save_data_h5(df2, name='{}_Y{}_Q{}'.format(tableName, yeari, quarteri),
                         subPath="{}/{}".format(dataBase_root_path, tableName), reWrite=reWrite)


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
        return tradeDateList[index_begin:index_end + 1]
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
    elif isinstance(date, datetime.date):
        return pd.to_datetime(date)
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


def extend_date_span(begin, end, freq) -> tuple[datetime.datetime, datetime.datetime]:
    """
    将区间[begin, end] 进行拓宽, 依据freq将拓展至指定位置, 详见下
    freq = M :
        [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-04-30]
        [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-04-30]
        [2018-01-04, 2018-04-30] -> [2018-01-01, 2018-04-30]
    freq = Q :
        [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-06-30]
        [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-06-30]
        [2018-01-04, 2018-06-30] -> [2018-01-01, 2018-06-30]
    freq = Y :
        [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-12-31]
        [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-12-31]
        [2018-01-04, 2018-12-31] -> [2018-01-01, 2018-12-31]
    """
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


def isdate(datestr, **kwargs):
    datestr = str(datestr)
    chinesenum = {'一': '1', '二': '2', '三': '3', '四': '4',
                  '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0', '十': '10'}
    strdate = ''
    for i in range(len(datestr)):
        temp = datestr[i]
        if temp in chinesenum:
            if temp == '十':
                if datestr[i + 1] not in chinesenum:
                    strdate += chinesenum[temp]
                elif datestr[i - 1] in chinesenum:
                    continue
                else:
                    strdate += '1'
            else:
                strdate += chinesenum[temp]
        else:
            strdate += temp

    pattern = ('%Y年%m月%d日',
               '%Y-%m-%d',
               '%y年%m月%d日',
               '%y-%m-%d',
               '%Y/%m/%d',
               '%Y%m%d'
               ) + kwargs.get('pattern', ())
    for i in pattern:
        try:
            ret = strptime(strdate, i)
            if ret:
                return True
        except:
            continue
    return False
