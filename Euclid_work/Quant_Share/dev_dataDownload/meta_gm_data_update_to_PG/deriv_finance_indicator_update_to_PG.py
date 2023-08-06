"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/23 11:13
# @Author  : Euclid-Jie
# @File    : deriv_finance_indicator_update_to_PG.py
# @Desc    : [衍生财务指标](https://www.myquant.cn/docs2/docs/#%E8%A1%8D%E7%94%9F%E8%B4%A2%E5%8A%A1%E6%8C%87%E6%A0%87)
             [get_fundamentals - 查询基本面数据](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-fundamentals-%E6%9F%A5%E8%AF%A2%E5%9F%BA%E6%9C%AC%E9%9D%A2%E6%95%B0%E6%8D%AE)

"""
from base_package import *

logger = logger_update_to_PG("deriv_finance_indicator")
# 获取2015年后的所有symbol
symbolList = pd.read_sql(
    """
    select symbol from stock_info where delisted_date >= '2015-01-01'
    """,
    con=postgres_engine(),
)["symbol"].values
deriv_finance_indicator_info = pd.read_excel(
    os.path.join(dev_files_dir, "deriv_finance_indicator.xlsx")
)
deriv_finance_indicator_fields = deriv_finance_indicator_info["列名"].to_list()

# 获取数据库中已有数据
exit_info = pd.read_sql(
    "select symbol, max(Date(update_time)) as date from deriv_finance_indicator group by symbol",
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
                INSERT INTO deriv_finance_indicator (symbol, pub_date)
                VALUES ('{}', '2015-01-01')""".format(
                    symbol
                ),
            )

        if format_date(begin) > get_tradeDate(end, -5):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = get_fundamentals(
                table="deriv_finance_indicator",
                symbols=symbol,
                limit=1000,
                start_date=begin,
                end_date=end,
                fields=deriv_finance_indicator_fields,
                df=True,
            )
            _len = len(data)
            if _len > 0:
                # TODO真离谱的, symbol和query不一致
                symbol = data["symbol"].values[0]
                for i in ["pub_date", "end_date"]:
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                data.columns = [col_i.lower() for col_i in data.columns]
                postgres_write_data_frame(
                    data,
                    "deriv_finance_indicator",
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
                table_name="deriv_finance_indicator",
                symbol=symbol,
                database="QS",
                time_column_name="update_time",
            )
