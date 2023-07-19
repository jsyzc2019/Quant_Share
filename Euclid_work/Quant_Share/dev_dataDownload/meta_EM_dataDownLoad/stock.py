from .log import c
from .utils import collate, check_status, Save_and_Log
import pandas as pd
from datetime import date
from Euclid_work.Quant_Share import get_tradeDates
from tqdm import tqdm

__all__ = ["stock_daily_csd"]


def stock_daily_csd(codes: list[str], start="2015-01-01", end=None, **kwargs):
    end = end if end else date.today().strftime("%Y-%m-%d")
    # 沪深股票 开盘价 收盘价 最高价 最低价 前收盘价 均价 涨跌 涨跌幅 是否涨停 成交量 成交金额 换手率 交易状态 成交笔数 是否跌停 振幅 内盘成交量 外盘成交量 复权因子（后） 前复权因子（定点复权） 是否为ST股票 是否为*ST股票
    indicators = "OPEN,CLOSE,HIGH,LOW,PRECLOSE,AVERAGE,CHANGE,PCTCHANGE,HIGHLIMIT,VOLUME,AMOUNT,TURN,TRADESTATUS,TNUM,LOWLIMIT,AMPLITUDE,BUYVOL,SELLVOL,TAFACTOR,FRONTTAFACTOR,ISSTSTOCK,ISXSTSTOCK"
    data = c.csd(
        codes,
        indicators,
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


def stock_daily_css(codes: str, start, end=None, **kwargs):
    # 沪深股票 前收盘价 开盘价 最高价 最低价 收盘价 均价 涨跌 涨跌幅 换手率 换手率(基准.自由流通股本) 成交量 成交额 振幅 成交笔数 相对发行价涨跌 相对发行价涨跌幅 收盘价(定点复权) 复权因子(后) 是否涨停 是否跌停 内盘成交量 外盘成交量 连续涨停天数 连续跌停天数 前复权因子（定点复权） 每笔平均成交股数 盈利比率 平均成本 个股第N个交易日 涨跌停状态 AH股溢价率 成交额(含大宗交易) 成交量(含大宗交易) 收盘集合竞价成交量 收盘集合竞价成交额
    indicators = "PRECLOSE,OPEN,HIGH,LOW,CLOSE,AVGPRICE,DIFFER,DIFFERRANGE,TURN,FREETURNOVER,VOLUME,AMOUNT,AMPLITUDE,TNUM,RELIPODIFFER,RELIPODIFFERRANGE,CLOSEB,TAFACTOR,ISSURGEDLIMIT,ISDECLINELIMIT,INPVOLUME,OUTPVOLUME,HLIMITEDAYS,LLIMITEDDAYS,FRONTTAFACTOR,AVGTVOL,PROFITRATIO,AVERAGECOST,NTRADEDAY,MAXUPORDOWN,AHPREMIUMRATE,AMOUNTBTIN,VOLUMEBTIN,CCAVOLUME,CCAAMOUNT"
    datelst = get_tradeDates(begin=start, end=end)
    print(
        f"Date from {datelst[0]} to {datelst[-1]}, actual number of days = {len(datelst)}"
    )

    res = pd.DataFrame()
    for _date in tqdm(datelst):
        date_str = _date.strftime("%Y-%m-%d")
        data = c.css(
            codes,
            indicators,
            f"TradeDate={date_str},AdjustFlag=1,BaseDate={date_str},Dbtype=1,N=0",
        )
        if check_status(data):
            df = pd.DataFrame(data.Data, index=data.Indicators).T
            df = df.reset_index(names="codes")
            df["tradeDate"] = date_str
            res = pd.concat([res, df], axis=0, ignore_index=True)
    return res
