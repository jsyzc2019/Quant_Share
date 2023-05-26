
import os
from EmQuantAPI import *
from installEmQuantAPI import *
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')
from Euclid_work.Quant_Share import stockList, tradeDateList, dataBase_root_path_EMdata, format_date
from Euclid_work.Quant_Share import patList, save_data_h5, get_table_info
from datetime import date
from tqdm import tqdm
from collections import OrderedDict
import json

from meta_dataDownLoad.save_gm_data_Y import save_gm_data_Y


def mainCallback(quantdata):
    """
    mainCallback 是主回调函数，可捕捉如下错误
    在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("mainCallback", str(quantdata))
    # 登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
    if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
        print("Your account is disconnect. You can force login automatically here if you need.")
    # 行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
        print("Your all csq subscribe have stopped.")
    # 行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002009":
        print("Your all csq subscribe have stopped, reconnect 6 times fail.")
    # 行情订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_QUOTE_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002012":
        print("csq subscribe break on some error, reconnect and request automatically.")
    # 资讯服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002014":
        print("Your all cnq subscribe have stopped, reconnect 6 times fail.")
    # 资讯订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_INFO_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002013":
        print("cnq subscribe break on some error, reconnect and request automatically.")
    # 资讯登录验证失败（每次连接资讯服务器时需要登录验证）或者资讯流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001024" or str(quantdata.ErrorCode) == "10001025":
        print("Your all cnq subscribe have stopped.")
    else:
        pass


def stock_daily(codes:list[str]=stockList,
                start="2015-01-01",
                end=None, **kwargs):
    end = end if end else date.today().strftime('%Y-%m-%d')
    # 沪深股票 开盘价 收盘价 最高价 最低价 前收盘价 均价 涨跌 涨跌幅 是否涨停 成交量 成交金额 换手率 交易状态 成交笔数 是否跌停 振幅 内盘成交量 外盘成交量 复权因子（后） 前复权因子（定点复权） 是否为ST股票 是否为*ST股票
    indicators = "OPEN,CLOSE,HIGH,LOW,PRECLOSE,AVERAGE,CHANGE,PCTCHANGE,HIGHLIMIT,VOLUME,AMOUNT,TURN,TRADESTATUS,TNUM,LOWLIMIT,AMPLITUDE,BUYVOL,SELLVOL,TAFACTOR,FRONTTAFACTOR,ISSTSTOCK,ISXSTSTOCK"
    data=c.csd(
        codes,
        indicators,
        start, end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    if (data.ErrorCode != 0):
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def filt_codes(codes: str or list[str], **kwargs):
    data = c.cec(codes, "ReturnType=0")
    if (data.ErrorCode != 0):
        print("request cec Error, ", data.ErrorMsg)
    else:
        res = []
        for key, value in data.Data.items():
            if value[2]: res.append(value[-1])
    return res


def collate(data: c.EmQuantData, index_name='tradeDate', columns_name='codes', **kwargs):
    final = pd.DataFrame()
    for i, ind in enumerate(data.Indicators):
        df = pd.DataFrame(
            dict([(k, v[i]) for k, v in data.Data.items()]), index=data.Dates
        )
        df.index.name = index_name
        df.columns.name = columns_name
        df = df.unstack()
        df.name = ind
        if i == 0:
            res = df
            continue
        res = pd.concat([res, df], axis=1)
    res = res.reset_index()
    final = pd.concat([final, res], ignore_index=True)
    return final


def index_daily(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime('%Y-%m-%d')
    # 2023-05-21 21:29:19
    # 指数 开盘价 收盘价 最高价 最低价 前收盘价 涨跌 涨跌幅 成交量 成交金额 换手率 振幅
    index_daily_indicators = "OPEN,CLOSE,HIGH,LOW,PRECLOSE,CHANGE,PCTCHANGE,VOLUME,AMOUNT,TURN,AMPLITUDE"
    data = c.csd(
        codes, index_daily_indicators, start, end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    if (data.ErrorCode != 0):
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def index_valuation(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime('%Y-%m-%d')
    # 2023-05-21 21:29:19 指数 总市值 流通市值 市盈率PE(TTM) 市盈率PE（最新年报） 市盈率PE中位值（TTM） 市盈率PE中位值（最新年报） 市净率PB中位值（MRQ） 市净率PB中位值（最新年报） 市净率PB中位值（最新公告） 市现率PCF(TTM) 市销率PS(TTM) 自由流通市值 股息率 市净率PB(
    # 最新年报) 市净率PB(MRQ) 股息率_TTM 股债性价比(差值)_序列 股债性价比(比值)_序列
    index_valuation_indicators = "MV,LIQMV,PETTM,PELYR,PEMIDTTM,PEMIDLYR,PBMIDMRQ,PBMIDLYR,PBMIDLF,PCFTTM,PSTTM,FREELIQMV,DIVIDENDYIELD,PBLYR,PBMRQ,DIVIDENDYIELDTTM,ERPMINUSM,ERPDIVIDEM"
    data = c.csd(
        codes, index_valuation_indicators, start, end,
        "DelType=1,EquityER=1,BondER=1,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    if (data.ErrorCode != 0):
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def index_financial(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime('%Y-%m-%d')
    # 2023-05-21 21:29:19
    # 指数 每股收益TTM 净资产收益率 流通股本 每股净资产BPS
    index_financial_indicators = "EPSTTM,ROE,LIQSHARE,BPS"
    data = c.csd(
        codes, index_financial_indicators, start, end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    if (data.ErrorCode != 0):
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def retrive_info(tableName:str):
    out = get_table_info(tableName)
    file_name_list = out['file_name_list']
    tableFolder = out['tableFolder']
    newest_file = file_name_list[-1]
    abs_file_path = os.path.join(tableFolder, newest_file)
    print(f'File path:{abs_file_path}')
    old_data = pd.read_hdf(abs_file_path)
    old_day = pd.to_datetime(old_data.tradeDate).max()
    old_day_off_1 = tradeDateList[tradeDateList.index(old_day)+1]
    new_day = date.today()
    print(f"原始最新日期:{old_day}")
    return old_data, abs_file_path, old_day_off_1, new_day

def update(codes, tableName: str, func):
    old_data, abs_file_path, old_day_off_1, new_day = retrive_info(tableName)
    if old_day_off_1 < new_day:
        new_data = func(codes=codes,
                       start=old_day_off_1.strftime('%Y-%m-%d'),
                       end=new_day.strftime('%Y-%m-%d'))
        if len(new_data) >= 1:
            all_data = pd.concat([old_data, new_data], axis=0)
            all_data.to_hdf(abs_file_path, 'a', 'w')
            print(f"{tableName}更新成功，最新日期{pd.to_datetime(new_data.tradeDate).max()}")
        else:
            print(f"{tableName}更新失败")
        pass
    else:
        print(f"{tableName}无需更新")


def batch_update(info, base:str, suffix:str, **kwargs):
    func = eval('_'.join([base, suffix]))
    for name, codes in info.items():
        tableName = '_'.join([name, suffix])
        update(codes, tableName=tableName, func=func)

def batch_download(info, base:str, suffix:str, **kwargs):
    current_dir = os.path.abspath(os.path.dirname(__file__))
    emTableJson = os.path.join(current_dir, '../dev_files/emData_tableInfo.json')
    func = eval('_'.join([base, suffix]))
    for name, codes in info.items():
        tableName = '_'.join([name, suffix])
        df = func(codes,
                  start=kwargs.get('start', '2015-01-01'),
                  end=kwargs.get('end',None)
                  )
        if df is not None:
            save_gm_data_Y(df, 'tradeDate', tableName, dataBase_root_path=dataBase_root_path_EMdata, reWrite=False)
            with open(emTableJson, 'r') as f:
                jsonDict = json.load(f, object_pairs_hook=OrderedDict)
            jsonDict[tableName] = {
                "assets": "emData",
                "description": "",
                "date_column": "tradeDate",
                "ticker_column": "codes"
            }
            with open(emTableJson, "w") as f:
                json.dump(jsonDict, f, indent=4, separators=(",", ": "))

def future_daily(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime('%Y-%m-%d')
    # 2023-05-21 21:29:19
    # 指数 开盘价 收盘价 最高价 最低价 前收盘价 涨跌 涨跌幅 成交量 成交金额 换手率 振幅
    future_daily_indicators = "OPEN,CLOSE,HIGH,LOW,PRECLOSE,AVERAGE,CHANGE,PCTCHANGE,VOLUME,AMOUNT,SPREAD,CLEAR,PRECLEAR,PCTCHANGECLEAR,CHANGECLEAR,HQOI,CHANGEOI,AMPLITUDE,MAINFORCE,UNIVOLUME,UNIAMOUNT,UNIHQOI,UNICHANGEOI,CHANGECLOSE,PCTCHANGECLOSE"
    data = c.csd(
        codes, future_daily_indicators, start, end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    if (data.ErrorCode != 0):
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df

def load_json(path:str):
    with open(path, 'r') as fp:
        return json.load(fp, object_pairs_hook=OrderedDict)

def check_status(data):
    if (data.ErrorCode != 0):
        print(data.ErrorMsg)
        return False
    else:
        return True

def CTR_index_download(indexcode='000300.SH',
                       EndDate="2023-05-25",
                       offset:int = 0,
                       **kwargs):
    # 2023-05-26 17:19:11
    # 该表主要提供指定日期的指数成分股代码及权重等信息 参数: 指数代码 截止日期 字段: 指数代码 成分代码 交易日期 成分名称 收盘价 涨跌幅 指数权重 指数贡献点 流通市值 总市值 流通股本 总股本
    if offset == 0:
        datelst = [EndDate]
    else:
        offset_day = c.getdate(EndDate, offset, "Market=CNSESH")
        offset_day = offset_day.Data[0]
        datelst = c.tradedates(offset_day, EndDate, "period=1,order=1,market=CNSESH")
        datelst = datelst.Data

    res = pd.DataFrame()
    for _date in tqdm(datelst):
        data = c.ctr("INDEXCOMPOSITION",
                     "INDEXCODE,SECUCODE,TRADEDATE,NAME,CLOSE,PCTCHANGE,WEIGHT,CONTRIBUTEPT,SHRMARKETVALUE,MV,TOTALTRADABLE,SHARETOTAL",
                     f"IndexCode={indexcode},EndDate={_date}")

        if check_status(data):
            tmp = pd.DataFrame(data.Data, index=data.Indicators)
            tmp = tmp.T
            res = pd.concat([res, tmp], axis=0, ignore_index=True)
    if len(res) > 0:
        tableName = indexcode.replace('.','')
        current_dir = os.path.abspath(os.path.dirname(__file__))
        emTableJson = os.path.join(current_dir, '../dev_files/emData_tableInfo.json')
        rewrite = kwargs.get('rewrite', True)
        save_gm_data_Y(res, 'TRADEDATE', tableName, dataBase_root_path=dataBase_root_path_EMdata, reWrite=rewrite)
        with open(emTableJson, 'r') as f:
            jsonDict = json.load(f, object_pairs_hook=OrderedDict)
        jsonDict[tableName] = {
            "assets": "emData",
            "description": "",
            "date_column": "TRADEDATE",
            "ticker_column": "SECUCODE"
        }
        with open(emTableJson, "w") as f:
            json.dump(jsonDict, f, indent=4, separators=(",", ": "))


stock_info = dict(

)

new_info = dict(
    # stock = stockList
)


if __name__ == '__main__':
    loginResult = c.start('ForceLogin=1', '', mainCallback)
    future_info = load_json('codes_info/future_info.json')
    index_info = load_json('codes_info/index_info.json')

    # DOWNLOAD
    # batch_download(new_info, 'stock', 'daily', start='2023-04-01')
    # batch_download(new_info, 'index', 'daily')
    # batch_download(new_info, 'index', 'financial')
    # batch_download(future_info, 'future', 'daily', start='2018-01-01', end='2022-12-31')

    # UPDATE
    # batch_update(index_info, 'index', 'daily')
    # batch_update(index_info, 'index', 'financial')

    CTR_index_download(indexcode='000300.SH', EndDate="2023-05-25", offset=-90)

    c.stop()




