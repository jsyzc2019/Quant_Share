
from meta_EM_dataDownLoad import c, load_json, collate, check_status, log
from meta_EM_dataDownLoad import batch_download, batch_update, update, Save_and_Log
from meta_EM_dataDownLoad import index_daily, index_financial
# from meta_EM_dataDownLoad import CTR_index_download
from tqdm import tqdm
import pandas as pd
from datetime import date
import json

def REITS(
        codes:list[str],
):
    pass

def get_tradeDate_range(
        Date:str,
        offset:str):
    if offset == 0:
        datelst = [Date]
    else:
        offset_day = c.getdate(Date, offset, "Market=CNSESH")
        offset_day = offset_day.Data[0]
        datelst = c.tradedates(offset_day, Date, "period=1,order=1,market=CNSESH")
        datelst = datelst.Data
    return datelst

def stock_daily_css(
    codes:str,
    Date:str,
    offset:int,
    tableName:str
):
    # 沪深股票 前收盘价 开盘价 最高价 最低价 收盘价 均价 涨跌 涨跌幅 换手率 换手率(基准.自由流通股本) 成交量 成交额 振幅 成交笔数 相对发行价涨跌 相对发行价涨跌幅 收盘价(定点复权) 复权因子(后) 是否涨停 是否跌停 内盘成交量 外盘成交量 连续涨停天数 连续跌停天数 前复权因子（定点复权） 每笔平均成交股数 盈利比率 平均成本 个股第N个交易日 涨跌停状态 AH股溢价率 成交额(含大宗交易) 成交量(含大宗交易) 收盘集合竞价成交量 收盘集合竞价成交额
    indicators = "PRECLOSE,OPEN,HIGH,LOW,CLOSE,AVGPRICE,DIFFER,DIFFERRANGE,TURN,FREETURNOVER,VOLUME,AMOUNT,AMPLITUDE,TNUM,RELIPODIFFER,RELIPODIFFERRANGE,CLOSEB,TAFACTOR,ISSURGEDLIMIT,ISDECLINELIMIT,INPVOLUME,OUTPVOLUME,HLIMITEDAYS,LLIMITEDDAYS,FRONTTAFACTOR,AVGTVOL,PROFITRATIO,AVERAGECOST,NTRADEDAY,MAXUPORDOWN,AHPREMIUMRATE,AMOUNTBTIN,VOLUMEBTIN,CCAVOLUME,CCAAMOUNT"
    datelst = get_tradeDate_range(Date, offset)
    print(f"Date from {datelst[0]} to {datelst[-1]}, actual number of days = {len(datelst)}")

    res = pd.DataFrame()
    for _date in tqdm(datelst):
        data = c.css(codes,indicators,f"TradeDate={_date},AdjustFlag=1,BaseDate={_date},Dbtype=1,N=0")
        if check_status(data):
            df = pd.DataFrame(data.Data, index=data.Indicators).T
            df = df.reset_index(names='codes')
            res = pd.concat([res, df], axis=0, ignore_index=True)
    res = res.dropna(axis=0, subset=['NTRADEDAY'])
    if len(res) > 0:
        Save_and_Log(res, tableName=tableName, date_column='NTRADEDAY', ticker_column='codes')



if __name__ == '__main__':
    # loginResult = c.start('ForceLogin=1', '', mainCallback)
    log()

    future_info = load_json('meta_EM_dataDownLoad/codes_info/future_info.json')
    stock_info = load_json('meta_EM_dataDownLoad/codes_info/stock.json')

    # for name in index_info.keys():
        # index_info[name]['tableName'] = ["_".join([name, f.split('_')[-1]]) for f in index_info[name]['func']]
        # index_info[name]['func'] = list(map(eval, index_info[name]['func']))

    # with open('meta_EM_dataDownLoad/codes_info/index_info.json', 'w') as fp:
    #     json.dump(index_info, fp, indent=2, separators=(",", ": "))

    # DOWNLOAD
    # batch_download(new_info, 'stock', 'daily', start='2023-04-01')
    # batch_download(new_info, 'index', 'daily')
    # batch_download(new_info, 'index', 'financial')
    # batch_download(future_info, 'future', 'daily', start='2018-01-01', end='2022-12-31')
    # date.today().strftime("%Y-%m-%d")
    # for key, val in stock_info.items():
    #     # code = val.split(',')[1]
    #     stock_daily_css(
    #         codes=val,
    #         Date='2023-5-25',
    #         offset=0,
    #         tableName="_".join([key, 'daily'])
    #     )

    # CTR_index_download(indexcode='000300.SH', EndDate="2023-05-25", offset=-90)

    c.stop()




