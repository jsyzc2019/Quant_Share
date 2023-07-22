"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 23:24
# @Author  : Euclid-Jie
# @File    : bench_info_update_to_PG.py
"""
from base_package import *

logger = logger_update_to_PG("bench_info")
end = date.today().strftime("%Y-%m-%d")

try:
    data = get_symbol_infos(sec_type1=1060, exchanges=["SHSE", "SZSE"], df=True)
    if len(data) > 0:
        for i in ["listed_date", "delisted_date"]:
            data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
        postgres_write_data_frame(
            data,
            "bench_info",
            update=True,
            unique_index=["symbol"],
            record_time=True,
        )
        logger.info("get {} itme(s)".format(len(data)))
except GmError:
    logger.error("GmError:{}".format(GmError))
