"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/21 22:53
# @Author  : Euclid-Jie
# @File    : gmData_bench_price_update_to_PG.py
# @Desc    : [get_history_symbol - 查询指定标的多日交易信息](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-history-symbol-%E6%9F%A5%E8%AF%A2%E6%8C%87%E5%AE%9A%E6%A0%87%E7%9A%84%E5%A4%9A%E6%97%A5%E4%BA%A4%E6%98%93%E4%BF%A1%E6%81%AF)
"""
from base_package import *

logger = logger_update_to_PG("gmData_bench_price")

exit_info = pd.read_sql(
    'select symbol, max(Date(record_time)) as date from "gmData_bench_price" group by symbol',
    con=postgres_engine(),
)
exit_info = exit_info.set_index("symbol")
with tqdm(bench_symbol_list) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = exit_info.loc[symbol]["date"].strftime("%Y-%m-%d")
        except AttributeError:
            # 说明目前有的表中, 有该symbol, 但是无数据, 设置其begin为2015-01-01
            begin = "2015-01-01"
        except KeyError:
            # 一般认为这种数据表中没有的symbol为2015-01-01前就退市, 可以直接continue, 不用获取数据
            begin = "2015-01-01"
            logger.info("{}:{}-{} skip".format(symbol, begin, end))
            continue

        if format_date(begin) > get_tradeDate(end, -1):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = get_history_symbol(symbol, start_date=begin, end_date=end, df=True)
            if len(data) > 0:
                for i in ["trade_date", "listed_date", "delisted_date"]:
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                postgres_write_data_frame(
                    data,
                    '"gmData_bench_price"',
                    update=True,
                    unique_index=["symbol", "trade_date"],
                    record_time=True,
                )
        except GmError:
            t.set_postfix({"状态": "GmError:{}".format(GmError)})
            logger.error("{}:{}-{} GmError:{}".format(symbol, begin, end, GmError))
            continue
        finally:
            t.set_postfix(
                {"状态": "{}:{}-{}写入{}条数据".format(symbol, begin, end, len(data))}
            )
            logger.info("{}:{}-{} get {} itme(s)".format(symbol, begin, end, len(data)))
