
from .utils import date, c, collate

def stock_daily_csd(codes:list[str],
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

