"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 11:03
# @Author  : Euclid-Jie
# @File    : fundamentals_cashflow_update_to_PG.py
# @Desc    : [stk_get_fundamentals_cashflow - 查询现金流量表数据](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#stk-get-fundamentals-cashflow-%E6%9F%A5%E8%AF%A2%E7%8E%B0%E9%87%91%E6%B5%81%E9%87%8F%E8%A1%A8%E6%95%B0%E6%8D%AE)
"""
from base_package import *

logger = logger_update_to_PG("fundamentals_cashflow")

fundamentals_cashflow_info = pd.read_excel(
    os.path.join(dev_files_dir, "fundamentals_cashflow_info.xlsx")
)
fundamentals_cashflow_fields = ",".join(
    fundamentals_cashflow_info["字段名"].to_list()[:-3]
)  # 最后三个字段没有
# 获取数据库中已有数据
exit_info = pd.read_sql(
    "select symbol, max(Date(update_time)) as date from fundamentals_cashflow group by symbol;",
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
                INSERT INTO fundamentals_cashflow (symbol, pub_date)
                VALUES ('{}',  '2015-01-01')""".format(
                    symbol
                ),
            )

        if format_date(begin) > get_tradeDate(end, -5):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = stk_get_fundamentals_cashflow(
                symbol=symbol,
                rpt_type=None,
                data_type=None,
                start_date=begin,
                end_date=end,
                fields=fundamentals_cashflow_fields,
                df=True,
            )
            _len = len(data)
            if _len > 0:
                symbol = data["symbol"].values[0]
                postgres_write_data_frame(
                    clean_data_frame_to_postgres(data, lower=True),
                    "fundamentals_cashflow",
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
                table_name="fundamentals_cashflow",
                symbol=symbol,
                database="QS",
                time_column_name="update_time",
            )
