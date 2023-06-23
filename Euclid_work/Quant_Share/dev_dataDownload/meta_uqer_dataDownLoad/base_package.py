import pandas as pd
from tqdm import tqdm
# 目前只能使用webAPI
# from uqer import DataAPI
# from uqer import Client
from Euclid_work.Quant_Share.Utils import stockNumList, format_date, save_data_h5, dataBase_root_path, extend_date_span
from Euclid_work.Quant_Share import get_data
import os
import time
from .FakeDataAPI import FakeDataAPI as DataAPI
# 路径识别
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
grand_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
dev_files_dir = os.path.join(grand_dir, 'dev_files')

# Uqer登录
# 通联登录
# with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
#     token = f.read().strip()
# 目前只能使用webAPI
# client = Client(token=token)
