"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 23:24
# @Author  : Euclid-Jie
# @File    : bench_info_update_to_PG.py
# @Desc    : [get_symbol_infos - 查询标的基本信息](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-symbol-infos-%E6%9F%A5%E8%AF%A2%E6%A0%87%E7%9A%84%E5%9F%BA%E6%9C%AC%E4%BF%A1%E6%81%AF)

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
