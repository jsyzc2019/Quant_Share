from .log import c
from .utils import collate, check_status, Save_and_Log
from Euclid_work.Quant_Share import get_tradeDates
import pandas as pd
from datetime import date
from tqdm import tqdm

__all__ = ["index_daily", "index_financial", "CTR_index_download"]


def index_daily(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime("%Y-%m-%d")
    # 2023-05-21 21:29:19
    # 指数 开盘价 收盘价 最高价 最低价 前收盘价 涨跌 涨跌幅 成交量 成交金额 换手率 振幅
    index_daily_indicators = (
        "OPEN,CLOSE,HIGH,LOW,PRECLOSE,CHANGE,PCTCHANGE,VOLUME,AMOUNT,TURN,AMPLITUDE"
    )
    data = c.csd(
        codes,
        index_daily_indicators,
        start,
        end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH",
    )
    if data.ErrorCode != 0:
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def index_valuation(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime("%Y-%m-%d")
    # 2023-05-21 21:29:19 指数 总市值 流通市值 市盈率PE(TTM) 市盈率PE（最新年报） 市盈率PE中位值（TTM） 市盈率PE中位值（最新年报） 市净率PB中位值（MRQ） 市净率PB中位值（最新年报） 市净率PB中位值（最新公告） 市现率PCF(TTM) 市销率PS(TTM) 自由流通市值 股息率 市净率PB(
    # 最新年报) 市净率PB(MRQ) 股息率_TTM 股债性价比(差值)_序列 股债性价比(比值)_序列
    index_valuation_indicators = "MV,LIQMV,PETTM,PELYR,PEMIDTTM,PEMIDLYR,PBMIDMRQ,PBMIDLYR,PBMIDLF,PCFTTM,PSTTM,FREELIQMV,DIVIDENDYIELD,PBLYR,PBMRQ,DIVIDENDYIELDTTM,ERPMINUSM,ERPDIVIDEM"
    data = c.csd(
        codes,
        index_valuation_indicators,
        start,
        end,
        "DelType=1,EquityER=1,BondER=1,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH",
    )
    if data.ErrorCode != 0:
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def index_financial(codes, start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime("%Y-%m-%d")
    # 2023-05-21 21:29:19
    # 指数 每股收益TTM 净资产收益率 流通股本 每股净资产BPS
    index_financial_indicators = "EPSTTM,ROE,LIQSHARE,BPS"
    data = c.csd(
        codes,
        index_financial_indicators,
        start,
        end,
        "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH",
    )
    if data.ErrorCode != 0:
        print("request csd Error, ", data.ErrorMsg)
        return
    else:
        df = collate(data)
        return df


def CTR_index_download(indexcode="000300.SH", start="2023-05-25", end=None, **kwargs):
    # 2023-05-26 17:19:11
    # 该表主要提供指定日期的指数成分股代码及权重等信息 参数: 指数代码 截止日期 字段: 指数代码 成分代码 交易日期 成分名称 收盘价 涨跌幅 指数权重 指数贡献点 流通市值 总市值 流通股本 总股本
    end = end if end else date.today().strftime("%Y%m%d")
    datelst = get_tradeDates(begin=start, end=end)
    res = pd.DataFrame()
    for _date in tqdm(datelst):
        data = c.ctr(
            "INDEXCOMPOSITION",
            "INDEXCODE,SECUCODE,TRADEDATE,NAME,CLOSE,PCTCHANGE,WEIGHT,CONTRIBUTEPT,SHRMARKETVALUE,MV,TOTALTRADABLE,SHARETOTAL",
            f"IndexCode={indexcode},EndDate={_date.strftime('%Y-%m-%d')}",
        )

        if check_status(data):
            tmp = pd.DataFrame(data.Data, index=data.Indicators)
            tmp = tmp.T
            res = pd.concat([res, tmp], axis=0, ignore_index=True)

    return res
