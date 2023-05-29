from meta_EM_dataDownLoad import c, load_json, collate, check_status, log
from meta_EM_dataDownLoad import batch_download, batch_update, update, Save_and_Log
from meta_EM_dataDownLoad import index_daily, index_financial
from tqdm import tqdm
import pandas as pd
from datetime import date
import json

if __name__ == '__main__':
    log()

    future_info = load_json('meta_EM_dataDownLoad/codes_info/future_info.json')
    stock_info = load_json('meta_EM_dataDownLoad/codes_info/stock.json')

    index_info = load_json('meta_EM_dataDownLoad/codes_info/index_info.json')
    for name in index_info.keys():
        index_info[name]['func'] = list(map(eval, index_info[name]['func']))
    batch_update(index_info)

    c.stop()
