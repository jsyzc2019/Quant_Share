import pandas as pd
from tqdm import tqdm

# 目前只能使用webAPI
# from uqer import DataAPI
# from uqer import Client

from Euclid_work.Quant_Share import get_data
from Euclid_work.Quant_Share import save_data_Y, save_data_Q, save_data_h5, dataBase_root_path, stockNumList
import os
import time
from .FakeDataAPI import FakeDataAPI as DataAPI
from datetime import date

# 路径识别
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
grand_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
dev_files_dir = os.path.join(grand_dir, "dev_files")
