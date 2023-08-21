"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/23 16:17
# @Author  : Euclid-Jie
# @File    : symbol_industry_sw21_update_to_PG.py
# @Desc    : 申万2021行业分类数据, 每月月底更新
            - [stk_get_industry_constituents - 查询行业成分股](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#stk-get-industry-constituents-%E6%9F%A5%E8%AF%A2%E8%A1%8C%E4%B8%9A%E6%88%90%E5%88%86%E8%82%A1)
            - use warehouse.load_latest_sw21_data to load data
"""
from base_package import *

logger = logger_update_to_PG("symbol_industry_sw21")
industry_category_info = pd.read_sql(
    "select industryid3 as industry_id from industry_category_info",
    con=postgres_engine(),
)
query_date = format_date(date.today())
least_query_date = pd.read_sql(
    "select max(query_date) from symbol_industry_sw21", con=postgres_engine()
).values[0][0]
if not query_date.is_month_end:
    query_date = query_date - pd.offsets.MonthEnd(n=1)
    if query_date == format_date(least_query_date):
        logger.info("{} has exited, skip".format(query_date.strftime("%Y-%m-%d")))
    else:
        all_industry_data = pd.DataFrame()
        for industry_id in tqdm(industry_category_info["industry_id"]):
            one_industry_data = stk_get_industry_constituents(
                industry_id, date=query_date.strftime("%Y-%m-%d")
            )
            one_industry_data["query_date"] = query_date.strftime("%Y-%m-%d")
            postgres_write_data_frame(
                one_industry_data,
                table_name="symbol_industry_sw21",
                update=True,
                unique_index=["symbol", "query_date"],
            )
            logger.info(
                "symbol_industry_sw21 {} get {} itme(s)".format(
                    industry_id, len(one_industry_data)
                )
            )
