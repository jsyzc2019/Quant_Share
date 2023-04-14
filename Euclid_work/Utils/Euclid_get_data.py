# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 21:38
# @Author  : Euclid-Jie
# @File    : Euclid_get_data.py
import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from joblib import Parallel, delayed
from .Utils import format_date, format_stockCode, format_futures, futures_list

dataBase_root_path = r'D:\Share\Euclid_work\dataFile'
dataBase_root_path_future = r"D:\Share\Fut_Data"
tableInfo = {
    'bench_price': {
        'assets': 'stock',
        'description': '',
        'date_column': 'trade_date',
        'ticker_column': 'symbol'
    },
    'stock_price': {
        'assets': 'stock',
        'description': '',
        'date_column': 'trade_date',
        'ticker_column': 'symbol'
    },
    'HKshszHold': {
        'assets': 'stock',
        'description': '',
        'date_column': 'endDate',
        'ticker_column': 'ticker'
    },
    'MktEqud': {
        'assets': 'stock',
        'description': '',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker'
    },
    # 'industry_info': {
    #     'description': '',
    #     'date_column': 'tradeDate',
    #     'ticker_column': 'aiq_ticker'
    # }
    'MktLimit': {
        'assets': 'stock',
        'description': '',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker'
    },

    'ResConSecCorederi': {
        'assets': 'stock',
        'description': '',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'FdmtDerPit': {
            'assets': 'stock',
            'description': '',
            'date_column': 'publishDate',
            'ticker_column': 'ticker'
        },
    # 期货数据组织形式的表
    'Broker_Data': {
        'assets': 'future',
        'description': '',
        'date_column': 'date',
        'ticker_column': ''
    },
    'Price_Volume_Data/main': {
        'assets': 'future',
        'description': '',
        'date_column': 'bob',
        'ticker_column': ''
    },
    'Price_Volume_Data/submain': {
        'assets': 'future',
        'description': '',
        'date_column': 'bob',
        'ticker_column': ''
    },
}


# 并行加速
def applyParallel(dfGrouped, function):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(function)(group) for name, group in tqdm(dfGrouped))
    return pd.concat(retLst)


# 带返回的并行调用
def run_thread_pool_sub(target, args, max_work_count):
    with ThreadPoolExecutor(max_workers=max_work_count) as t:
        res = [t.submit(target, i) for i in args]
        return res


def load_file(toLoadList):
    load_data = pd.DataFrame()
    res = run_thread_pool_sub(pd.read_hdf, toLoadList, max_work_count=10)
    for future in as_completed(res):
        res = future.result()
        # 拼接数据
        if isinstance(res.index[0], datetime) or pd.to_datetime(res.index[0]).year >= 2015:
            res = res.reset_index()
        load_data = pd.concat((load_data, res), axis=0, ignore_index=True)
    return load_data


def get_data(tabelName, begin='20150101', end=None, sources='gm', fields: list = None, ticker: list = None):
    """
    :param tabelName: bench_info / bench_price / stock_info / stock_price / tradeDate_info / HKshszHold
    :param begin:
    :param end:
    :param sources:
    :param fields:
    :param ticker:
    :return:
    """
    if not end:
        end = datetime.today().now().strftime('%Y%m%d')

    begin = format_date(begin)
    end = format_date(end)

    if tabelName not in list(tableInfo.keys()):
        raise KeyError("{} is not ready for use!".format(tabelName))

    if tableInfo[tabelName]['assets'] == 'stock':
        return get_data_stock(tabelName, begin, end, fields, ticker)
    elif tableInfo[tabelName]['assets'] == 'future':
        return get_data_future(tabelName, begin, end, sources, fields, ticker)


def get_data_stock(tabelName, begin, end, fields, ticker):
    tabelFoldPath = os.path.join(dataBase_root_path, tabelName)
    if not os.path.exists(tabelFoldPath):
        try:
            data = pd.read_hdf(tabelFoldPath + '.h5')
            return data
        except FileNotFoundError:
            print("{} no exits!".format(tabelName))

    h5_file_name_list = os.listdir(tabelFoldPath)
    # 如果文件是季度组织的
    if 'Q' in h5_file_name_list[0]:
        load_begin = begin - pd.tseries.offsets.QuarterBegin(0)
        load_end = end + pd.tseries.offsets.QuarterEnd(0)
        toLoadList = []
        for fileName in ["{}_Y{}_Q{:.0f}.h5".format(tabelName, QuarterEnd.year, QuarterEnd.month / 3) for QuarterEnd in pd.date_range(load_begin, load_end, freq='q')]:
            if fileName not in h5_file_name_list:
                raise AttributeError("{} is not exit!".format(fileName))
            else:
                filePath = os.path.join(tabelFoldPath, fileName)
                toLoadList.append(filePath)
        load_data = load_file(toLoadList)
        return selectFields(load_data, tabelName, begin, end, fields, ticker)
    # 按照年组织
    elif "Y" in h5_file_name_list[0]:
        load_begin = begin - pd.tseries.offsets.YearBegin(0)
        load_end = end + pd.tseries.offsets.YearEnd(0)
        toLoadList = []
        for fileName in ["{}_Y{}.h5".format(tabelName, YearEnd.year) for YearEnd in pd.date_range(load_begin, load_end, freq='Y')]:
            if fileName not in h5_file_name_list:
                raise AttributeError("{} is not exit!".format(fileName))
            else:
                filePath = os.path.join(tabelFoldPath, fileName)
                toLoadList.append(filePath)
        load_data = load_file(toLoadList)
        return selectFields(load_data, tabelName, begin, end, fields, ticker)
    else:
        raise KeyError("请检查{}, 文件不符合{}_Y*_Q*组织形式".format(tabelFoldPath, tabelName))


def get_data_future(tabelName, begin='20160101', end=None, sources='gm', fields: list = None, ticker: list = None):
    if not end:
        end = datetime.today().now().strftime('%Y%m%d')
    tabelFoldPath = os.path.join(dataBase_root_path_future, tabelName)
    begin = format_date(begin)
    end = format_date(end)

    load_begin = begin - pd.tseries.offsets.YearBegin(0)
    load_end = end + pd.tseries.offsets.YearEnd(0)
    toLoadList = []
    for year in [YearEnd.year for YearEnd in pd.date_range(load_begin, load_end, freq='Y')]:
        tmpFolder = os.path.join(tabelFoldPath, str(year), sources)
        tmpFileList = os.listdir(tmpFolder)
        target = [x for x in tmpFileList if ticker == format_futures(x)]
        if target.__len__() > 0:
            toLoadList.extend([os.path.join(tmpFolder, target[0])])
    if toLoadList.__len__() > 0:
        load_data = load_file(toLoadList)
        return selectFields(load_data, tabelName, begin, end, fields, ticker)
    else:
        raise KeyError("未找到{}文件".format(tabelName))


def selectFields(data, tabelName, begin, end, fields: list = None, ticker: list = None):
    outData = data.copy()
    date_column = tableInfo[tabelName]['date_column']
    ticker_column = tableInfo[tabelName]['ticker_column']
    # date filter
    if ticker_column != '':
        outData.sort_values(by=[ticker_column, date_column], ascending=[1, 1], inplace=True)
    else:
        outData.sort_values(by=[date_column], ascending=True, inplace=True)
    if isinstance(outData[date_column][0], int):
        dateSpan = pd.to_datetime(outData[date_column], format='%Y%m%d')
    else:
        dateSpan = pd.to_datetime(outData[date_column]).apply(lambda x: x.replace(tzinfo=None))
    outData = outData.loc[(dateSpan >= format_date(begin)).values & (dateSpan <= format_date(end)).values]
    # ticker filter
    if ticker and ticker not in futures_list:
        tickerSpan = outData[ticker_column].apply(lambda x: format_stockCode(x))
        outData = outData[tickerSpan.isin([format_stockCode(x) for x in ticker])]

    if fields:
        if set(fields).issubset(outData.columns):
            outData = outData[fields]
        else:
            notExitField = "\\".join(list(set(fields) - set(outData.columns)))
            raise AttributeError("{} is not exit in {}".format(notExitField, tabelName))
    return outData.reset_index(drop=True)
