"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/1 22:06
# @Author  : Euclid-Jie
# @File    : MktAdj.py
"""
from .base_package import *
from Euclid_work.Quant_Share.warehouse import *


def MktAdj(begin, end, **kwargs):
    """
    沪深股票前复权因子
    columns:
        secID	String	通联编制的证券编码，可使用getSecID接口获取到
        ticker	String	通用交易代码
        exchangeCD	String	通联编制的交易市场编码
        secShortName	String	证券简称
        secShortNameEn	String	证券英文简称
        exDivDate	Date	除权除息日，股改对应股改后首个交易日
        perCashDiv	Double	每股派现（税前）
        perShareDivRatio	Double	每股送股比例
        perShareTransRatio	Double	每股转增股比例
        allotmentRatio	Double	每股配股比例
        allotmentPrice	Double	配股价
        adjFactor	Double	单次前复权因子，对应一个除权除息日的权息修复比例
        accumAdjFactor	Double	累积前复权因子，发生在本次除权除息日（含）之后的前复权因子累乘。每当有新的前复权因子产生，该股票的所有累积前复权因子均会刷新
        endDate	Date	累积前复权因子起始生效日期，前复权价=对应交易日期在[endDate,exDivDate)区间的未复权价*累积前复权因子
        updateTime	Date	最近一次更新时间
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.MktAdjfGet(
        secID="",
        ticker=kwargs["ticker"],
        beginDate=begin,
        endDate=end,
        pandas="1",
    )
    return data


if __name__ == '__main__':
    symbolList = pd.read_sql(
        "select sec_id  from stock_info where delisted_date >= '2015-01-01'",
        con=postgres_engine(),
    )["sec_id"].values
    data = MktAdj("2015-01-01", "2023-08-01", ticker=stockNumList)
    write_df_to_pgDB(data, table_name="uqer_mkt_adj", database="QS", engine=postgres_engine())
