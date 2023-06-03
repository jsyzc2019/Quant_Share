from meta_EM_dataDownLoad import c, load_json, collate, check_status, log
from meta_EM_dataDownLoad import batch_download, batch_update, update, Save_and_Log
from meta_EM_dataDownLoad import index_daily, index_financial, future_daily, stock_daily_csd
from tqdm import tqdm
import pandas as pd
from datetime import date
import json
from collections import ChainMap

if __name__ == '__main__':
    log()

    future_info = load_json('meta_EM_dataDownLoad/codes_info/future.json')
    stock_info = load_json('meta_EM_dataDownLoad/codes_info/stock.json')
    index_info = load_json('meta_EM_dataDownLoad/codes_info/index.json')

    all_info = ChainMap(index_info, future_info, stock_info)

    for name in all_info.keys():
        all_info[name]['func'] = list(map(eval, all_info[name]['func']))

    batch_update(all_info)

    c.stop()
