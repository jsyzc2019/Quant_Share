import pandas as pd
import os
from gm.api import *
from Euclid_work.Quant_Share import \
    format_date, patList, save_data_h5, dataBase_root_path_gmStockFactor, \
    stock_info, get_tradeDates, bench_info, save_data_Q, save_data_Y
from tqdm import tqdm
import time

# 路径识别
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
grand_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
dev_files_dir = os.path.join(grand_dir, 'dev_files')

# Gm登录
with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
    token = f.read().strip()
set_token(token)
symbolList = list(stock_info.symbol.unique())
