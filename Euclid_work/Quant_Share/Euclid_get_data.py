# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 21:38
# @Author  : Euclid-Jie
# @File    : Euclid_get_data.py
import os
import time

import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from joblib import Parallel, delayed
from .Utils import format_date, format_stockCode, format_futures, futures_list, dataBase_root_path, dataBase_root_path_future, dataBase_root_path_gmStockFactor
from .tableInfo import tableInfo


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
        if len(res) > 0:
            # 拼接数据
            if isinstance(res.index[0], datetime) or pd.to_datetime(res.index[0]).year >= 2015:
                res = res.reset_index()
            load_data = pd.concat((load_data, res), axis=0, ignore_index=True)
    return load_data


def get_data(tableName, begin='20150101', end=None, sources='gm', fields: list = None, ticker: list = None):
    """
    :param tableName: bench_info / bench_price / stock_info / stock_price / tradeDate_info / HKshszHold
    :param begin:
    :param end:
    :param sources:
    :param fields:
    :param ticker:
    :return:
    """
    if tableName not in list(tableInfo.keys()):
        raise KeyError("{} is not ready for use!".format(tableName))

    if not end:
        end = datetime.today().now().strftime('%Y%m%d')

    begin = format_date(begin)
    end = format_date(end)

    if not isinstance(ticker, list):
        if isinstance(ticker, str):
            ticker = [ticker]

    tableAssets = tableInfo[tableName]['assets']
    if tableAssets == 'stock':
        return get_data_stock(tableName, begin, end, fields, ticker)
    elif tableAssets == 'future':
        return get_data_future(tableName, begin, end, sources, fields, ticker)
    elif tableAssets == 'info':
        return get_data_stock(tableName, begin, end, fields, ticker)
    elif tableAssets == 'gmStockFactor':
        return get_data_gmStockFactor(tableName, begin, end, fields, ticker)


def get_data_stock(tableName, begin, end, fields, ticker):
    tabelFoldPath = os.path.join(dataBase_root_path, tableName)
    if not os.path.exists(tabelFoldPath):
        try:
            data = pd.read_hdf(tabelFoldPath + '.h5')
            return data
        except FileNotFoundError:
            print("{} no exits!".format(tableName))

    h5_file_name_list = os.listdir(tabelFoldPath)
    # 如果文件是季度组织的
    if 'Q' in h5_file_name_list[0]:
        load_begin = begin - pd.tseries.offsets.QuarterBegin(0)
        load_end = end + pd.tseries.offsets.QuarterEnd(0)
        toLoadList = []
        for fileName in ["{}_Y{}_Q{:.0f}.h5".format(tableName, QuarterEnd.year, QuarterEnd.month / 3) for QuarterEnd in pd.date_range(load_begin, load_end, freq='q')]:
            if fileName not in h5_file_name_list:
                raise AttributeError("{} is not exit!".format(fileName))
            else:
                filePath = os.path.join(tabelFoldPath, fileName)
                toLoadList.append(filePath)
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker)
    # 按照年组织
    elif "Y" in h5_file_name_list[0]:
        load_begin = begin - pd.tseries.offsets.YearBegin(0)
        load_end = end + pd.tseries.offsets.YearEnd(0)
        toLoadList = []
        for fileName in ["{}_Y{}.h5".format(tableName, YearEnd.year) for YearEnd in pd.date_range(load_begin, load_end, freq='Y')]:
            if fileName not in h5_file_name_list:
                raise AttributeError("{} is not exit!".format(fileName))
            else:
                filePath = os.path.join(tabelFoldPath, fileName)
                toLoadList.append(filePath)
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker)
    else:
        raise KeyError("请检查{}, 文件不符合{}_Y*_Q*组织形式".format(tabelFoldPath, tableName))


def get_data_future(tableName, begin='20160101', end=None, sources='gm', fields: list = None, ticker: list = None):
    tabelFoldPath = os.path.join(dataBase_root_path_future, tableName)

    load_begin = begin - pd.tseries.offsets.YearBegin(0)
    load_end = end + pd.tseries.offsets.YearEnd(0)
    toLoadList = []
    for year in [YearEnd.year for YearEnd in pd.date_range(load_begin, load_end, freq='Y')]:
        tmpFolder = os.path.join(tabelFoldPath, str(year), sources)
        tmpFileList = os.listdir(tmpFolder)
        # 已在此处进行ticker筛选，不在selectFields中进行
        if ticker:
            target = [x for x in tmpFileList if format_futures(x) in ticker]
        else:
            target = tmpFileList
        if target.__len__() > 0:
            toLoadList.extend([os.path.join(tmpFolder, target_i) for target_i in target])
    if toLoadList.__len__() > 0:
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker=None)
    else:
        raise KeyError("未找到{}文件".format(tableName))


def get_data_gmStockFactor(tableName, begin='20160101', end=None, fields: list = None, ticker: list = None):
    # D:\Share\Stk_Data\gm\ACCA
    tabelFoldPath = os.path.join(dataBase_root_path_gmStockFactor, tableName)

    load_begin = begin - pd.tseries.offsets.YearBegin(0)
    load_end = end + pd.tseries.offsets.YearEnd(0)
    toLoadList = []
    for year in [YearEnd.year for YearEnd in pd.date_range(load_begin, load_end, freq='Y')]:
        tmpFolder = os.path.join(tabelFoldPath, str(year))  # D:\Share\Stk_Data\gm\ACCA\2013
        target = os.listdir(tmpFolder)  # D:\Share\Stk_Data\gm\ACCA\2013\ACCA.h5
        if target.__len__() > 0:
            toLoadList.extend([os.path.join(tmpFolder, target_i) for target_i in target])
    if toLoadList.__len__() > 0:
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker)
    else:
        raise KeyError("未找到{}文件".format(tableName))


def selectFields(data, tableName, begin, end, fields: list = None, ticker: list = None):
    outData = data.copy()
    date_column = tableInfo[tableName]['date_column']
    ticker_column = tableInfo[tableName]['ticker_column']
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
    if ticker:
        tickerSpan = outData[ticker_column].apply(lambda x: format_stockCode(x))
        outData = outData[tickerSpan.isin([format_stockCode(x) for x in ticker])]

    if fields:
        if set(fields).issubset(outData.columns):
            outData = outData[fields]
        else:
            notExitField = "\\".join(list(set(fields) - set(outData.columns)))
            raise AttributeError("{} is not exit in {}".format(notExitField, tableName))
    return outData.reset_index(drop=True)


def get_all_file(folderPath, query='.h5'):
    # 得到所有文件
    FileList = []
    file_full_path_list = []
    for path, file_dir, files in os.walk(folderPath):
        FileList.extend([file_name for file_name in files if file_name.endswith(query)])
        file_full_path_list.extend([os.path.join(path, file_name) for file_name in files if file_name.endswith(query)])
    return FileList, file_full_path_list


def get_tablePath_info(tablePath):
    # if table has single h5 file
    if not os.path.exists(tablePath):
        tableFolder = dataBase_root_path_future
        tablePath = tablePath + '.h5'
        # file not exits!
        if not os.path.isfile(tablePath):
            raise FileNotFoundError("{} no exits!".format(tablePath))
        file_name_list = os.path.split(tablePath)[-1]
        file_full_path_list = [tablePath]
        return tableFolder, file_name_list, file_full_path_list
    else:  # table has a folder
        file_name_list, file_full_path_list = get_all_file(tablePath)
        tableFolder = tablePath
    return tableFolder, file_name_list, file_full_path_list


def get_table_info(tableName):
    if tableName not in list(tableInfo.keys()):
        raise KeyError("{} is not ready for use!".format(tableName))
    tableSource = tableInfo[tableName]['tableSource']
    description = tableInfo[tableName]['description']

    # tablePath should be a folder or a .h5 file
    if tableSource == 'gmFuture':
        tablePath = os.path.join(dataBase_root_path_future, tableName)
        tableFolder, file_name_list, file_full_path_list = get_tablePath_info(tablePath)

    elif tableSource == 'gmStockFactor':
        tablePath = os.path.join(dataBase_root_path_gmStockFactor, tableName)
        tableFolder, file_name_list, file_full_path_list = get_tablePath_info(tablePath)

    else:  # InfoTable, dataYesStock, gmStock
        tablePath = os.path.join(dataBase_root_path, tableName)
        tableFolder, file_name_list, file_full_path_list = get_tablePath_info(tablePath)

    # stat time
    """
    atime Access Time 访问时间 最后一次访问文件（读取或执行）的时间
    ctime Change Time 变化时间 最后一次改变文件（属性或权限）或者目录（属性或权限）的时间
    mtime Modify Time 修改时间 最后一次修改文件（内容）或者目录（内容）的时间
    """

    stat_list = [os.stat(file_full_path) for file_full_path in file_full_path_list]
    st_atime = max(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_atime)) for stat in stat_list)
    st_ctime = max(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_ctime)) for stat in stat_list)
    st_mtime = max(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime)) for stat in stat_list)
    outJson = {
        'tableSource': tableSource,
        'description': description,
        'tableFolder': tableFolder,
        # 'tablePath': tablePath,
        'file_name_list': file_name_list,
        'Access Time': st_atime,
        'Change Time': st_ctime,
        'Modify Time': st_mtime,

    }

    return outJson