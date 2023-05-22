import time
from datetime import datetime as dt

import numpy as np
import pandas as pd
from gm.api import *
import sys
from Euclid_work.Quant_Share import stock_info
from tqdm import tqdm
from Euclid_work.Quant_Share import save_data_h5, dataBase_root_path_gmStockFactor, format_date, patList, tradeDateList
from meta_dataDownLoad import future_daily, save_gm_data_Y

begin = format_date('20150101')
end = format_date('20231231')
tradeDateArr = [i for i in tradeDateList if begin <= i <= end]
data = future_daily(tradeDateArr=tradeDateArr)
save_gm_data_Y(data, 'trade_date', 'future_daily', reWrite=True)
