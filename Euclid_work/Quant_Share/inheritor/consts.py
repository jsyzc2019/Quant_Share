from typing import List, Any

import pandas as pd
from .utils import format_stock


class Config:
    database_dir = {
        "root": r"E:\Share\Stk_Data\dataFile",
        "future": r"E:\Share\Fut_Data",
        "gm_stock_factor": r"E:\Share\Stk_Data\gm",
        "em_stock_factor": r"E:\Share\EM_Data",
        "jq_stock_factor": r"E:\Share\JointQuant_Factor",
        "jq_data_prepare": r"E:\Share\JointQuant_prepare",
    }

    datasets_name = list(database_dir.keys())

    stock_table = pd.read_hdf("{}/stock_info.h5".format(database_dir["root"]))
    stock_list = list(map(format_stock, stock_table["symbol"]))
    stock_num_list = stock_table["sec_id"].unique().tolist()

    futures_list: list[str | Any] = (
        "AG",
        "AL",
        "AU",
        "A",
        "BB",
        "BU",
        "B",
        "CF",
        "CS",
        "CU",
        "C",
        "FB",
        "FG",
        "HC",
        "IC",
        "IF",
        "IH",
        "I",
        "JD",
        "JM",
        "JR",
        "J",
        "LR",
        "L",
        "MA",
        "M",
        "NI",
        "OI",
        "PB",
        "PM",
        "PP",
        "P",
        "RB",
        "RI",
        "RM",
        "RS",
        "RU",
        "SF",
        "SM",
        "SN",
        "SR",
        "TA",
        "TF",
        "T",
        "V",
        "WH",
        "Y",
        "ZC",
        "ZN",
        "PG",
        "EB",
        "AP",
        "LU",
        "SA",
        "TS",
        "CY",
        "IM",
        "PF",
        "PK",
        "CJ",
        "UR",
        "NR",
        "SS",
        "FU",
        "EG",
        "LH",
        "SP",
        "RR",
        "SC",
        "WR",
        "BC",
    )

    trade_date_table = pd.read_hdf("{}/tradeDate_info.h5".format(database_dir["root"]))
    trade_date_list = trade_date_table["tradeDate"].dropna().to_list()

    quarter_begin = ["0101", "0401", "0701", "1001"]
    quarter_end = ["0331", "0630", "0930", "1231"]
