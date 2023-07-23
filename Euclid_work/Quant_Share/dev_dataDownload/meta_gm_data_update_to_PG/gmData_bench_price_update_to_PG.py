"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/21 22:53
# @Author  : Euclid-Jie
# @File    : gmData_bench_price_update_to_PG.py
# @Desc    : 更新gmData_bench_price, 基于record_time
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
        except KeyError:
            # 一般认为这种数据表中没有的symbol为2015-01-01前就退市, 可以直接continue, 不用获取数据
            begin = "2015-01-01"
            logger.info("{}:{}-{} skip".format(symbol, begin, end))
            # continue

        if format_date(begin) > get_tradeDate(end, -5):
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
