from EmQuantAPI import *
from installEmQuantAPI import *
import datetime as dt
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
from Euclid_work.Quant_Share import stockList, tradeDateList, dataBase_root_path_EMdata, patList, format_date
from Euclid_work.Quant_Share import patList
from meta_dataDownLoad import save_gm_data_Y
from Euclid_work.Quant_Share import get_table_info
import os
from datetime import date

stock_ind = ["CLOSE","OPEN","HIGH",'LOW','PRECLOSE','AVERAGE', 'CHANGE',
              'PCTCHANGE','VOLUME','HIGHLIMIT','AMOUNT',
              'TURN','LOWLIMIT','AMPLITUDE','TNUM','TAFACTOR',
              'FRONTTAFACTOR','ISSTSTOCK','ISXSTSTOCK']

def mainCallback(quantdata):
    """
    mainCallback 是主回调函数，可捕捉如下错误
    在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("mainCallback", str(quantdata))
    #登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
    if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
        print("Your account is disconnect. You can force login automatically here if you need.")
    #行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
        print("Your all csq subscribe have stopped.")
    #行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
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
def stock_daily(date_to_store:list,
                codes:list[str]=stockList,
                indicators:list[str]=stock_ind,
                **kwargs):
    with tqdm(patList(date_to_store, 30)) as t:
        final = pd.DataFrame()
        for patDates in t:
            t.set_description("begin:{} -- end:{}".format(patDates[0], patDates[-1]))
            try:
                data = c.csd(
                    codes,
                    indicators,
                    patDates[0], patDates[-1],
                    "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
                for i, ind in enumerate(indicators):
                    df = pd.DataFrame(
                        dict([(k, v[i]) for k, v in data.Data.items()]), index=patDates
                    )
                    df.index.name = 'tradeDate'
                    df.columns.name = 'ticker'
                    df = df.unstack()
                    df.name = ind
                    if i == 0:
                        res = df
                        continue
                    res = pd.concat([res, df], axis=1)
                res = res.reset_index()
                final = pd.concat([final, res], ignore_index=True)
            except:
                continue
    return final

def filt_codes(codes:str or list[str], **kwargs):
    data = c.cec(codes, "ReturnType=0")
    if (data.ErrorCode != 0):
        print("request cec Error, ", data.ErrorMsg)
    else:
        res = []
        for key, value in data.Data.items():
            if value[2]: res.append(value[-1])
    return res

def collate(data:c.EmQuantData, index_name='tradeDate', columns_name='codes', **kwargs):
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

def stock_daily_update(**kwargs):

    pass


SHCOMP_codes = "000001.SH,000002.SH,000003.SH,000004.SH,000005.SH,000006.SH,000007.SH,000008.SH,000017.SH,000020.SH,000090.SH,000688.SH,000688HKD00.SH,000688HKD01.SH,000688USD00.SH,000688USD01.SH,000688USD07.SH,000688USD08.SH,930928.CSI,H20928.CSI,N20928.CSI"

Shanghai_scale_index_codes = "000009.SH,000010.SH,000016.SH,000043.SH,000044.SH,000045.SH,000046.SH,000047.SH,000155.SH,H00009.SH,H00010.SH,H00016.SH,H00043.SH,H00044.SH,H00045.SH,H00046.SH,H00047.SH,H00132.SH,H00133.SH,H00155.SH,N00009.SH,N00010.SH,N00016.SH,N00043.SH,N00044.SH,N00045.SH,N00046.SH,N00047.SH,N00132.SH,N00133.SH"

def index_daily(codes, start="2015-01-01", end="2023-05-21", **kwargs):
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

def index_valuation(codes, start="2015-01-01", end="2023-05-21", **kwargs):
    # 2023-05-21 21:29:19
    # 指数 总市值 流通市值 市盈率PE(TTM) 市盈率PE（最新年报） 市盈率PE中位值（TTM） 市盈率PE中位值（最新年报） 市净率PB中位值（MRQ） 市净率PB中位值（最新年报） 市净率PB中位值（最新公告） 市现率PCF(TTM) 市销率PS(TTM) 自由流通市值 股息率 市净率PB(最新年报) 市净率PB(MRQ) 股息率_TTM 股债性价比(差值)_序列 股债性价比(比值)_序列
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

def index_financial(codes, start="2015-01-01", end="2023-05-21", **kwargs):
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

def update_daily(codes, tableName:str):
    global Trade
    out = get_table_info(tableName)
    file_name_list = out['file_name_list']
    tableFolder = out['tableFolder']
    newest_file = file_name_list[-1]
    abs_file_path = os.path.join(tableFolder, newest_file)
    old_data = pd.read_hdf(abs_file_path)
    old_day = format_date(old_data.tradeDate.max())
    old_day_off_1 = Trade[np.where(Trade == old_day)[0]+1][0]
    new_day = format_date(date.today().strftime('%Y-%m-%d'))
    if old_day_off_1 < new_day:
        new_data = index_daily(codes=codes,
                               start=pd.to_datetime(old_day_off_1).strftime('%Y-%m-%d'),
                               end=new_day.strftime('%Y-%m-%d'))
        if len(new_data) >= 1:
            all_data = pd.concat([old_data, new_data], axis=0)
            all_data.to_hdf(abs_file_path, 'a', 'w')
            print(f"{tableName}更新成功，最新日期{pd.to_datetime(new_data.tradeDate).max()}")
        else:
            print(f"{tableName}更新失败")
    else:
        print(f"{tableName}无需更新")




if __name__ == '__main__':
    loginResult = c.start('ForceLogin=1', '', mainCallback)
    # begin = format_date('20150101')
    # end = format_date('20230101')
    Trade = np.sort(pd.to_datetime(tradeDateList))
    # tradeDateArr = Trade[(begin <= Trade) & (Trade <= end)]

    # df = index_daily(SHCOMP_codes, start='20230101')
    # if df is not None: save_gm_data_Y(df, 'tradeDate', 'SHCOMP_daily', dataBase_root_path=dataBase_root_path_EMdata, reWrite=True)
    #
    # df = index_daily(Shanghai_scale_index_codes, start='20230101')
    # if df is not None: save_gm_data_Y(df, 'tradeDate', 'Shanghai_scale_index_daily', dataBase_root_path=dataBase_root_path_EMdata, reWrite=True)

    # df = index_valuation(start="2022-01-01", end="2023-05-21")
    # if df is not None: save_gm_data_Y(df, 'tradeDate', 'SHCOMP_valuation', dataBase_root_path=dataBase_root_path_EMdata, reWrite=True)

    # df = index_financial(SHCOMP_codes)
    # if df is not None: save_gm_data_Y(df, 'tradeDate', 'SHCOMP_financial', dataBase_root_path=dataBase_root_path_EMdata, reWrite=True)

    # df = index_financial(Shanghai_scale_index_codes)
    # if df is not None: save_gm_data_Y(df, 'tradeDate', 'Shanghai_scale_index_financial', dataBase_root_path=dataBase_root_path_EMdata, reWrite=True)

    # update_daily(codes=SHCOMP_codes, tableName='SHCOMP_daily')

    # update_daily(codes=Shanghai_scale_index_codes, tableName='Shanghai_scale_index_daily')



