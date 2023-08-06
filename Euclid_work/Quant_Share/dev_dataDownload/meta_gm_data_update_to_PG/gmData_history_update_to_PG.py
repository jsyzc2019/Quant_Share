"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/20 9:41
# @Author  : Euclid-Jie
# @File    : gmData_history_update_to_PG.py
# @Desc    : [history - 查询历史行情](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#history-%E6%9F%A5%E8%AF%A2%E5%8E%86%E5%8F%B2%E8%A1%8C%E6%83%85)
            - 更新gmData_history(1d), 基于record_time
"""
from base_package import *

logger = logger_update_to_PG("gmData_history")

exit_info = pd.read_sql(
    'select symbol, max(Date(bob)) as date from "gmData_history" group by symbol',
    con=postgres_engine(),
)
exit_info = exit_info.set_index("symbol")
with tqdm(symbolList) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = exit_info.loc[symbol]["date"].strftime("%Y-%m-%d")
        except KeyError:
            # 一般认为这种数据表中没有的symbol为新上市, begin设置为2015-01-01
            begin = "2015-01-01"

        if format_date(begin) >= get_tradeDate(end, -1):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = history(
                symbol, frequency="1d", start_time=begin, end_time=end, df=True
            )
            if len(data) > 0:
                for i in ["bob", "eob"]:
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                postgres_write_data_frame(
                    data,
                    '"gmData_history"',
                    update=True,
                    unique_index=["symbol", "bob"],
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
