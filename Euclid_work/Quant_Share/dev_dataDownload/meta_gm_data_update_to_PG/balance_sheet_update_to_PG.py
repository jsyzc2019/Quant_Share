"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/21 17:56
# @Author  : Euclid-Jie
# @File    : balance_sheet_update_to_PG.py
# @Desc    : [资产负债表](https://www.myquant.cn/docs2/docs/#%E8%B5%84%E4%BA%A7%E8%B4%9F%E5%80%BA%E8%A1%A8)
             [get_fundamentals - 查询基本面数据](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-fundamentals-%E6%9F%A5%E8%AF%A2%E5%9F%BA%E6%9C%AC%E9%9D%A2%E6%95%B0%E6%8D%AE)
"""
from base_package import *

logger = logger_update_to_PG("balance_sheet")

balance_sheet_info = pd.read_excel(os.path.join(dev_files_dir, "balance_sheet.xlsx"))
balance_sheet_fields = balance_sheet_info["列名"].to_list()

# 由balance_sheet_latest_info获取上一次更新时间
# 更新时间begin的选取, 应为update_time
exit_info = pd.read_sql(
    "select symbol, max(Date(update_time)) as date from balance_sheet group by symbol;",
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
            # 说明目前有的表中, 没有该symbol, 设置其begin为2015-01-01
            begin = "2015-01-01"
            postgres_cur_execute(
                database="QS",
                sql_text="""
                INSERT INTO balance_sheet (symbol, pub_date)
                VALUES ('{}', '2015-01-01')""".format(
                    symbol
                ),
            )

        if format_date(begin) > get_tradeDate(end, -5):
            # 设置更新周期, 相较于record time
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = get_fundamentals(
                table="balance_sheet",
                symbols=symbol,
                limit=1000,
                start_date=begin,
                end_date=end,
                fields=balance_sheet_fields,
                df=True,
            )
            _len = len(data)
            if _len > 0:
                # TODO 贼离谱, 查出来的symbol并不是query中的symbol
                symbol = data["symbol"].values[0]
                # 记录数据
                postgres_write_data_frame(
                    clean_data_frame_to_postgres(
                        data, ["pub_date", "end_date"], lower=True
                    ),
                    "balance_sheet",
                    update=True,
                    unique_index=["symbol", "pub_date", "end_date"],
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
                table_name="balance_sheet",
                symbol=symbol,
                database="QS",
                time_column_name="update_time",
            )
