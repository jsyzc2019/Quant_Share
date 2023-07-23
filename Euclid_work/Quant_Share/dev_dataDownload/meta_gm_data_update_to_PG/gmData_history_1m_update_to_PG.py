"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/4 9:47
# @Author  : Euclid-Jie
# @File    : gmData_history_1m_update_to_PG.py
# @Desc    : [history - 查询历史行情](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#history-%E6%9F%A5%E8%AF%A2%E5%8E%86%E5%8F%B2%E8%A1%8C%E6%83%85)
            - 用于下载/更新60s bar数据数据至数据库
            - 更新适用于短日期间隔
            - 长间隔使用gmData_history_1m_download.py

"""
from base_package import *

logger = logger_update_to_PG("gmData_history_1m")

# gmData_history_1m原始数据表过大, 更新需要使用QS_log.gmData_history_1m_latest_info
exit_info = pd.read_sql(
    'select symbol, latest as date from "gmData_history_1m_latest_info";',
    con=postgres_engine(database="QS_log"),
)
exit_info = exit_info.set_index("symbol")

with tqdm(symbolList) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = (exit_info.loc[symbol]["date"] + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
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
            data = history(
                symbol, frequency="60s", start_time=begin, end_time=end, df=True
            )
            _len = len(data)
            if _len > 0:
                for i in ["bob", "eob"]:
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")

                symbol = data["symbol"].values[0]
                data.columns = [col_i.lower() for col_i in data.columns]
                postgres_write_data_frame(
                    data,
                    '"gmData_history_1m"',
                    update=False,
                )
                latest_info = data[["symbol", "bob"]].sort_values(["symbol", "bob"])
                latest_info = latest_info.drop_duplicates(keep="last").rename(
                    columns={"bob": "latest"}
                )
                postgres_write_data_frame(
                    latest_info,
                    '"gmData_history_1m_latest_info"',
                    update=True,
                    unique_index=["symbol"],
                    record_time=True,
                    database="QS_log",
                )
        except GmError:
            t.set_postfix({"状态": "GmError:{}".format(GmError)})
            logger.error("{}:{}-{} GmError:{}".format(symbol, begin, end, GmError))
            continue
        finally:
            t.set_postfix({"状态": "{}:{}-{}写入{}条数据".format(symbol, begin, end, _len)})
            logger.info("{}:{}-{} get {} itme(s)".format(symbol, begin, end, _len))
