"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/20 9:41
# @Author  : Euclid-Jie
# @File    : gmData_history_update_to_PG.py
# @Desc    : 更新gmData_history(1d), 基于record_time
"""
from base_package import *

logger = logging.getLogger("gmData_history")


exit_info = pd.read_sql(
    'select symbol, max(Date(record_time)) as date from "gmData_history" group by symbol',
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
