"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 9:15
# @Author  : Euclid-Jie
# @File    : fundamentals_balance_update_to_PG.py
# @Desc    : [stk_get_fundamentals_balance - 查询资产负债表数据](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#stk-get-fundamentals-balance-%E6%9F%A5%E8%AF%A2%E8%B5%84%E4%BA%A7%E8%B4%9F%E5%80%BA%E8%A1%A8%E6%95%B0%E6%8D%AE)
            接口限制: 每1000限流一次
"""
from base_package import *

logger = logger_update_to_PG("fundamentals_balance")
# 获取2015年后的所有symbol
symbolList = pd.read_sql(
    """
    select symbol from stock_info where delisted_date >= '2015-01-01'
    """,
    con=postgres_engine(),
)["symbol"].values

fundamentals_balance_info = pd.read_excel(
    os.path.join(dev_files_dir, "fundamentals_balance_info.xlsx")
)
fundamentals_balance_fields = ",".join(fundamentals_balance_info["字段名"].to_list())

# 获取数据库中已有数据, 基于上一次的更新时间进行本次更新
exit_info = pd.read_sql(
    "select symbol, max(Date(update_time)) as date from fundamentals_balance group by symbol;",
    con=postgres_engine(),
)
exit_info = exit_info.set_index("symbol")

with tqdm(symbolList) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = exit_info.loc[symbol]["date"].strftime("%Y-%m-%d")
        except AttributeError:
            # 说明目前有的表中, 有该symbol, 但是无数据, 设置其begin为2015-01-01
            begin = "2015-01-01"
        except KeyError:
            # 说明目前有的表中, 没有该symbol, 设置其begin/pub_date为2015-01-01
            begin = "2015-01-01"
            postgres_cur_execute(
                database="QS",
                sql_text="""
                INSERT INTO fundamentals_balance (symbol, pub_date)
                VALUES ('{}',  '2015-01-01')""".format(
                    symbol
                ),
            )

        if format_date(begin) > get_tradeDate(end, -5):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = stk_get_fundamentals_balance(
                symbol=symbol,
                rpt_type=None,
                data_type=None,
                start_date=begin,
                end_date=end,
                fields=fundamentals_balance_fields,
                df=True,
            )
            _len = len(data)
            if _len > 0:
                symbol = data["symbol"].values[0]
                postgres_write_data_frame(
                    clean_data_frame_to_postgres(data, lower=True),
                    "fundamentals_balance",
                    update=True,
                    unique_index=["symbol", "pub_date", "rpt_date"],
                    record_time=True,
                )
        except GmError:
            t.set_postfix({"状态": "GmError:{}".format(GmError)})
            logger.error("{}:{}-{} GmError:{}".format(symbol, begin, end, GmError))
            _len = -1
        finally:
            t.set_postfix({"状态": "{}:{}-{}写入{}条数据".format(symbol, begin, end, _len)})
            logger.info("{}:{}-{} get {} itme(s)".format(symbol, begin, end, _len))
            update_time(
                table_name="fundamentals_balance",
                symbol=symbol,
                database="QS",
                time_column_name="update_time",
            )
