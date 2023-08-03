from meta_EM_dataDownLoad import (
    c,
    load_json,
    collate,
    check_status,
    log,
    map_func,
    get_tradeDate_range,
)
from meta_EM_dataDownLoad import batch_download, batch_update, update, Save_and_Log
from meta_EM_dataDownLoad import index_daily, index_financial, stock_daily_csd
from meta_EM_dataDownLoad import CTR_index_download, future_daily
from tqdm import tqdm
import pandas as pd
from datetime import date
import json


if __name__ == "__main__":
    log()

    future_info = load_json("meta_EM_dataDownLoad/codes_info/future.json")
    stock_info = load_json("meta_EM_dataDownLoad/codes_info/stock.json")
    index_info = load_json("meta_EM_dataDownLoad/codes_info/index.json")

    new_info = load_json("meta_EM_dataDownLoad/codes_info/new.json")

    for name in new_info.keys():
        new_info[name]["func"] = list(map(eval, new_info[name]["func"]))

    batch_download(new_info, start="2022-01-01", end="2022-12-31")

    c.stop()
