import pandas as pd
from gm.api import *
from Euclid_work.Quant_Share import format_date, patList, save_data_h5, dataBase_root_path_gmStockFactor, stock_info
from tqdm import tqdm
import time

# Gm登录
# with open('token.txt', 'rt', encoding='utf-8') as f:
#     token = f.read().strip()
# set_token(token)
# symbolList = list(stock_info.symbol.unique())
