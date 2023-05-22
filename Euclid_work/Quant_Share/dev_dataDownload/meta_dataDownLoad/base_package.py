import pandas as pd
import os
from gm.api import *
from Euclid_work.Quant_Share import format_date, patList, save_data_h5, dataBase_root_path_gmStockFactor, stock_info
from tqdm import tqdm
import time

# Gm登录
current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
    token = f.read().strip()
set_token(token)
symbolList = list(stock_info.symbol.unique())
