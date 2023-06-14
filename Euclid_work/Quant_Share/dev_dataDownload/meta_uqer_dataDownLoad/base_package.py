import pandas as pd
from tqdm import tqdm
from uqer import DataAPI, Client
from Euclid_work.Quant_Share.Utils import stockNumList, format_date, save_data_h5, dataBase_root_path, extend_date_span
import os
import time

# 路径识别
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
grand_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
dev_files_dir = os.path.join(grand_dir, 'dev_files')

# Uqer登录
# 通联登录
with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
    token = f.read().strip()
client = Client(token=token)
