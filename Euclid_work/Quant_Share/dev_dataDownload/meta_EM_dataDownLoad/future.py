
from .log import c
from .utils import collate, check_status, Save_and_Log
import pandas as pd
from datetime import date

__all__ = [
    'future_daily'
]

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