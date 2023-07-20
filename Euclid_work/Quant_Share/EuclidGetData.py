# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 21:38
# @Author  : Euclid-Jie
# @File    : EuclidGetData.py
import json
import multiprocessing
import os
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from fuzzywuzzy import process
from joblib import Parallel, delayed
from pandas.errors import ParserError
from tqdm import tqdm

from .TableInfo import tableInfo
from .Utils import dataBase_root_path_EM_data
from .Utils import (
    format_date,
    format_stockCode,
    format_futures,
    dataBase_root_path,
    dataBase_root_path_future,
    dataBase_root_path_gmStockFactor,
    extend_date_span,
    isdate,
    dataBase_root_path_JointQuant_prepare,
)

warnings.filterwarnings("ignore")
__all__ = ["get_data", "get_table_info", "search_keyword", "get_data_Base"]

table_MAP = {
    "info": dataBase_root_path,
    "stock": dataBase_root_path,
    "gmFuture": dataBase_root_path_future,
    "gmStockFactor": dataBase_root_path_gmStockFactor,
    "gmStockData": dataBase_root_path_gmStockFactor,
    "emData": dataBase_root_path_EM_data,
    "jointquant": dataBase_root_path_JointQuant_prepare,
}


# 并行加速
def applyParallel(dfGrouped, function):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(function)(group) for name, group in tqdm(dfGrouped)
    )
    return pd.concat(retLst)


# 带返回的并行调用
def run_thread_pool_sub(target, args, max_work_count):
    with ThreadPoolExecutor(max_workers=max_work_count) as t:
        res = [t.submit(target, i) for i in args]
        return res


def read_data_h5(h5FileFullPath, key="a"):
    return pd.read_hdf(h5FileFullPath, key=key)


def load_file(toLoadList):
    load_data = pd.DataFrame()
    res = run_thread_pool_sub(read_data_h5, toLoadList, max_work_count=20)
    for future in as_completed(res):
        res = future.result()
        if len(res) > 0:
            # 拼接数据
            try:
                if isinstance(res.index[0], datetime) or isdate(res.index[0]):
                    res = res.reset_index()
            except ParserError:
                res = res.reset_index(drop=True)
            load_data = pd.concat((load_data, res), axis=0, ignore_index=True)
    return load_data


def get_data(
    tableName,
    begin="20150101",
    end=None,
    sources="gm",
    fields: list = None,
    ticker: list = None,
    **kwargs
):
    """
    :param tableName: bench_info / bench_price / stock_info / stock_price / tradeDate_info / HKshszHold
    :param begin:
    :param end:
    :param sources:
    :param fields:
    :param ticker:
    :return:
    """
    global table_MAP
    if tableName not in list(tableInfo.keys()):
        raise KeyError("{} is not ready for use!".format(tableName))

    if end is None:
        end = datetime.today().now().strftime("%Y%m%d")

    if begin:
        begin = format_date(begin)
    end = format_date(end)

    if not isinstance(ticker, list):
        if isinstance(ticker, (str, int)):
            ticker = [ticker]

    tableAssets = tableInfo[tableName]["assets"]
    if tableAssets in ["stock", "info", "emData", "gmStockData", "jointquant"]:
        return get_data_Base(
            tableName, begin, end, fields, ticker, table_MAP[tableAssets], **kwargs
        )
    elif tableAssets == "future":
        return get_data_future(tableName, begin, end, sources, fields, ticker)
    elif tableAssets == "gmStockFactor":
        return get_data_gmStockFactor(tableName, begin, end, fields, ticker)


def get_data_Base(tableName, begin, end, fields, ticker, path, **kwargs):
    tableFoldPath = os.path.join(path, tableName)
    if not os.path.exists(tableFoldPath):
        try:
            data: pd.DataFrame | object = pd.read_hdf(tableFoldPath + ".h5")
            return selectFields(data, tableName, begin, end, fields, ticker)
        except FileNotFoundError:
            print("{} no exits!".format(tableName))

    h5_file_name_list = os.listdir(tableFoldPath)
    # 如果文件是季度组织的
    if begin:
        if "Q" in h5_file_name_list[0]:
            load_begin, load_end = extend_date_span(begin, end, "Q")
            toLoadList = []
            for fileName in [
                "{}_Y{}_Q{:.0f}.h5".format(
                    tableName, QuarterEnd.year, QuarterEnd.month / 3
                )
                for QuarterEnd in pd.date_range(load_begin, load_end, freq="q")
            ]:
                if fileName not in h5_file_name_list:
                    warnings.warn("{} is not exit!".format(fileName))
                    # raise AttributeError("{} is not exit!".format(fileName))
                else:
                    filePath = os.path.join(tableFoldPath, fileName)
                    toLoadList.append(filePath)
        # 按照年组织
        elif "Y" in h5_file_name_list[0]:
            load_begin, load_end = extend_date_span(begin, end, "Y")
            toLoadList = []
            for fileName in [
                "{}_Y{}.h5".format(tableName, YearEnd.year)
                for YearEnd in pd.date_range(load_begin, load_end, freq="Y")
            ]:
                if fileName not in h5_file_name_list:
                    raise AttributeError("{} is not exit!".format(fileName))
                else:
                    filePath = os.path.join(tableFoldPath, fileName)
                    toLoadList.append(filePath)
        else:
            raise KeyError("请检查{}, 文件不符合{}_Y*_Q*组织形式".format(tableFoldPath, tableName))
    else:
        toLoadList = [
            os.path.join(tableFoldPath, filename) for filename in h5_file_name_list
        ]
    load_data = load_file(toLoadList)
    return selectFields(
        load_data,
        tableName,
        begin,
        end,
        fields,
        ticker,
        verbose=kwargs.get("verbose", True),
    )


def get_data_future(
    tableName,
    begin="20160101",
    end=None,
    sources="gm",
    fields: list = None,
    ticker: list = None,
):
    tableFoldPath = os.path.join(dataBase_root_path_future, tableName)
    load_begin, load_end = extend_date_span(begin, end, "Y")
    toLoadList = []
    for year in [
        YearEnd.year for YearEnd in pd.date_range(load_begin, load_end, freq="Y")
    ]:
        tmpFolder = os.path.join(tableFoldPath, str(year), sources)
        tmpFileList = [
            tmpFile for tmpFile in os.listdir(tmpFolder) if tmpFile.endswith(".h5")
        ]
        # 已在此处进行ticker筛选，不在selectFields中进行
        if ticker:
            target = [x for x in tmpFileList if format_futures(x) in ticker]
        else:
            target = tmpFileList
        if target.__len__() > 0:
            toLoadList.extend(
                [os.path.join(tmpFolder, target_i) for target_i in target]
            )
    if toLoadList.__len__() > 0:
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker=None)
    else:
        raise KeyError("未找到{}文件".format(tableName))


def get_data_gmStockFactor(
    tableName, begin="20160101", end=None, fields: list = None, ticker: list = None
):
    tableFoldPath = os.path.join(dataBase_root_path_gmStockFactor, tableName)
    load_begin, load_end = extend_date_span(begin, end, "Y")
    toLoadList = []
    for year in [
        YearEnd.year for YearEnd in pd.date_range(load_begin, load_end, freq="Y")
    ]:
        tmpFolder = os.path.join(
            tableFoldPath, str(year)
        )  # D:\Share\Stk_Data\gm\ACCA\2013
        target = os.listdir(tmpFolder)  # D:\Share\Stk_Data\gm\ACCA\2013\ACCA.h5
        if target.__len__() > 0:
            toLoadList.extend(
                [os.path.join(tmpFolder, target_i) for target_i in target]
            )
    if toLoadList.__len__() > 0:
        load_data = load_file(toLoadList)
        return selectFields(load_data, tableName, begin, end, fields, ticker)
    else:
        raise KeyError("未找到{}文件".format(tableName))


def selectFields(
    data,
    tableName,
    begin,
    end,
    fields: list = None,
    ticker: list = None,
    verbose: bool = False,
):
    outData = data.copy()
    # get date_column, ticker_column
    try:
        date_column = tableInfo[tableName]["date_column"]
        outData[date_column] = pd.to_datetime(outData[date_column])
    except KeyError:
        if verbose:
            print("{} 不支持时间筛选".format(tableName))
        date_column = ""
    try:
        ticker_column = tableInfo[tableName]["ticker_column"]
    except KeyError:
        if verbose:
            print("{} 不支持ticker筛选".format(tableName))
        ticker_column = ""

    # data filter: date -> ticker -> fields
    if date_column != "" and ticker_column != "":
        # outData.sort_values(by=[date_column, ticker_column], ascending=[1, 1], inplace=True)
        outData = selectFields_dateSpan(outData, begin, end, date_column)
        if ticker:
            outData = selectFields_ticker(outData, ticker, ticker_column)
    elif date_column != "":
        # outData.sort_values(by=[date_column], ascending=True, inplace=True)
        outData = selectFields_dateSpan(outData, begin, end, date_column)
    elif ticker_column != "":
        # outData.sort_values(by=[ticker_column], ascending=True, inplace=True)
        if ticker:
            outData = selectFields_ticker(outData, ticker, ticker_column)
    else:
        # 不进行任何筛选排序
        pass
    # finally data filter: fields
    if fields:
        if set(fields).issubset(outData.columns):
            outData = outData[fields]
        else:
            notExitField = "\\".join(list(set(fields) - set(outData.columns)))
            raise AttributeError("{} is not exit in {}".format(notExitField, tableName))
    return outData.reset_index(drop=True)


def selectFields_ticker(Data, ticker, ticker_column):
    # ticker filter
    tickerSpan = Data[ticker_column].apply(lambda x: format_stockCode(x))
    outData = Data[tickerSpan.isin([format_stockCode(x) for x in ticker])]
    return outData


def selectFields_dateSpan(Data, begin, end, date_column):
    if isinstance(Data[date_column][0], int):
        Data[date_column] = pd.to_datetime(Data[date_column], format="%Y%m%d")
    else:
        # 提速可能会报错
        # dateSpan = pd.to_datetime(Data[date_column]).apply(lambda x: x.replace(tzinfo=None))
        Data[date_column] = pd.to_datetime(Data[date_column]).dt.tz_localize(None)
    if begin:
        Data = Data[Data[date_column] >= format_date(begin)]
    if end:
        Data = Data[Data[date_column] <= format_date(end)]
    # outData = Data.loc[(dateSpan >= format_date(begin)) & (dateSpan <= format_date(end))]
    return Data


def get_all_file(folderPath, query=".h5"):
    # 得到所有文件
    FileList = []
    file_full_path_list = []
    for path, file_dir, files in os.walk(folderPath):
        FileList.extend([file_name for file_name in files if file_name.endswith(query)])
        file_full_path_list.extend(
            [
                os.path.join(path, file_name)
                for file_name in files
                if file_name.endswith(query)
            ]
        )
    return FileList, file_full_path_list


def get_tablePath_info(tablePath):
    # if table has single h5 file
    if not os.path.exists(tablePath):
        tableFolder = dataBase_root_path_future
        tablePath = tablePath + ".h5"
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
    global table_MAP
    if tableName not in list(tableInfo.keys()):
        raise KeyError("{} is not ready for use!".format(tableName))
    tableSource = tableInfo[tableName]["tableSource"]
    description = tableInfo[tableName]["description"]
    date_column = tableInfo[tableName]["date_column"]
    ticker_column = tableInfo[tableName]["ticker_column"]

    if tableSource in table_MAP:
        tablePath = os.path.join(table_MAP[tableSource], tableName)
        tableFolder, file_name_list, file_full_path_list = get_tablePath_info(tablePath)
    else:
        tablePath = os.path.join(dataBase_root_path, tableName)
        tableFolder, file_name_list, file_full_path_list = get_tablePath_info(tablePath)

    # stat time
    """
    atime Access Time 访问时间 最后一次访问文件（读取或执行）的时间
    ctime Change Time 变化时间 最后一次改变文件（属性或权限）或者目录（属性或权限）的时间
    mtime Modify Time 修改时间 最后一次修改文件（内容）或者目录（内容）的时间
    """

    stat_list = [os.stat(file_full_path) for file_full_path in file_full_path_list]
    st_atime = max(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_atime))
        for stat in stat_list
    )
    st_ctime = max(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_ctime))
        for stat in stat_list
    )
    st_mtime = max(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime))
        for stat in stat_list
    )
    outJson = {
        "tableSource": tableSource,
        "description": description,
        "tableFolder": tableFolder,
        "date_column": date_column,
        "ticker_column": ticker_column,
        # 'tablePath': tablePath,
        "file_name_list": file_name_list,
        "Access Time": st_atime,
        "Change Time": st_ctime,
        "Modify Time": st_mtime,
    }

    return outJson


def search_keyword(keyword: str, fuzzy=True, limit=5, update: bool = False):
    """
    :param keyword: the content you want to search for
    :param fuzzy: fuzzy matching or not
    :param limit: number of the results
    :param update: forced updating
    :return:
    """
    # attrsMap.json check
    current_dir = os.path.abspath(os.path.dirname(__file__))
    attrsMapPath = os.path.join(current_dir, "dev_files/attrsMap.json")
    if not os.path.exists(attrsMapPath) or update:
        attrsMap = defaultdict(list)
        with tqdm(tableInfo.keys()) as t:
            t.set_description("attrsMap正在初始化...")
            for tableName in t:
                try:
                    _df = get_data(tableName, begin="20230101")
                    _col = list(_df.columns)
                    for c in _col:
                        attrsMap[c].append(tableName)
                    t.set_postfix({"状态": "{} 写入成功".format(tableName)})
                except FileNotFoundError as e:
                    t.set_postfix({"状态": "Warning {} not found".format(e.filename)})
            with open(attrsMapPath, "w") as write_file:
                json.dump(attrsMap, write_file, indent=4)
    else:
        with open(attrsMapPath, "r") as read_file:
            attrsMap = json.load(read_file)

    dic = {}
    if fuzzy:
        res = process.extract(keyword, attrsMap.keys(), limit=limit)
        for candiate, score in res:
            dic[candiate] = attrsMap[candiate]
            # print(f"{candiate} in {attrsMap[candiate]}")
    else:
        res = list(filter(lambda x: keyword in x or keyword == x, attrsMap.keys()))
        for candiate in res:
            dic[candiate] = attrsMap[candiate]
            # print(f"{candiate} in {attrsMap[candiate]}")

    return dic
