"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 8:58
# @Author  : Euclid-Jie
# @File    : share_change_update_to_PG.py
"""
from base_package import *
logger = logger_update_to_PG("share_change")

# 获取数据库中已有数据
exit_info = pd.read_sql(
    "select symbol, max(Date(record_time)) as date from share_change group by symbol;",
    con=postgres_engine(),
)
exit_info = exit_info.set_index("symbol")
with tqdm(symbolList) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = exit_info.loc[symbol]["date"].strftime("%Y-%m-%d")
        except KeyError:
            # 一般认为这种数据表中没有的symbol为2015-01-01前就退市, 可以直接continue, 不用获取数据
            # begin = "2015-01-01"
            logger.info("{}:{}-{} skip".format(symbol, begin, end))
            continue

        if format_date(begin) > get_tradeDate(end, -5):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = stk_get_share_change(symbol=symbol, start_date=begin, end_date=end)
            if len(data) > 0:
                symbol = data["symbol"].values[0]
                data.columns = [col_i.lower() for col_i in data.columns]
                postgres_write_data_frame(data, "share_change", update=True, unique_index=["symbol", "pub_date", "chg_date"], record_time=True)
        except GmError:
            t.set_postfix({"状态": "GmError:{}".format(GmError)})
            logger.error("{}:{}-{} GmError:{}".format(symbol, begin, end, GmError))
            continue
        finally:
            t.set_postfix(
                {"状态": "{}:{}-{}写入{}条数据".format(symbol, begin, end, len(data))}
            )
            logger.info("{}:{}-{} get {} itme(s)".format(symbol, begin, end, len(data)))
