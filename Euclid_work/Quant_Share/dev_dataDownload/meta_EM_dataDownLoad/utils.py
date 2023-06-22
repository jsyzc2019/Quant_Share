from .log import *
import os
import pandas as pd
from Euclid_work.Quant_Share import get_table_info, tradeDateList, dataBase_root_path_EM_data
from datetime import date
import json
from Euclid_work.Quant_Share import save_data_Y
from collections import OrderedDict


def filt_codes(codes: str or list[str], **kwargs):
    data = c.cec(codes, "ReturnType=0")
    if (data.ErrorCode != 0):
        print("request cec Error, ", data.ErrorMsg)
    else:
        res = []
        for key, value in data.Data.items():
            if value[2]: res.append(value[-1])
    return res

def collate(data: c.EmQuantData, index_name='tradeDate', columns_name='codes', **kwargs):
    for i, ind in enumerate(data.Indicators):
        df = pd.DataFrame(
            dict([(k, v[i]) for k, v in data.Data.items()]), index=data.Dates
        )
        df.index.name = index_name
        df.columns.name = columns_name
        df = df.unstack()
        df.name = ind
        if i == 0:
            res = df
        else:
            res = pd.concat([res, df], axis=1)
    res = res.reset_index()
    return res

def retrive_info(tableName:str):
    out = get_table_info(tableName)
    file_name_list = out['file_name_list']
    tableFolder = out['tableFolder']
    date_column = out['date_column']
    newest_file = file_name_list[-1]
    abs_file_path = os.path.join(tableFolder, newest_file)
    print(f'File path:{abs_file_path}')
    old_data = pd.read_hdf(abs_file_path)
    old_day = pd.to_datetime(old_data[date_column]).max()
    old_day_off_1 = tradeDateList[tradeDateList.index(old_day)+1]
    new_day = date.today()
    print(f"原始最新日期:{old_day}")
    return old_data, abs_file_path, old_day_off_1, new_day, date_column

def update(codes, tableName: str, func, **kwargs):
    old_data, abs_file_path, old_day_off_1, new_day, date_column = retrive_info(tableName)
    flag = True
    if old_day_off_1 < new_day:
        new_data = func(codes=codes,
                       start=old_day_off_1.strftime('%Y-%m-%d'),
                       end=new_day.strftime('%Y-%m-%d'))
        if new_data is not None and len(new_data) >= 1:
            save_data_Y(new_data, date_column, tableName, dataBase_root_path=dataBase_root_path_EM_data, reWrite=True)
            # all_data = pd.concat([old_data, new_data], axis=0, ignore_index=True)
            # all_data.to_hdf(abs_file_path, 'a', 'w')
            print(f"{tableName}更新成功，最新日期{pd.to_datetime(new_data[date_column]).max()}")
        else:
            print(f"{tableName}更新失败")
            flag = False
    else:
        print(f"{tableName}无需更新")
    return flag

def map_func(infos):
    for name in infos.keys():
        infos[name]['func'] = list(map(lambda x:eval(x, globals(), locals()), infos[name]['func']))
    return infos

def batch_update(infos, **kwargs):
    fail_lst = []
    for name, info in infos.items():
        codes = info["codes"]
        for func, tableName in zip(info["func"], info["tableName"]):
            flag = update(codes, tableName=tableName, func=func, **kwargs)
            if not flag: fail_lst.append(tableName)
    if fail_lst:
        print(f"以下数据表更新失败:\n{fail_lst}")

def batch_download(infos, **kwargs):
    for name, info in infos.items():
        codes = info["codes"]
        for func, tableName in zip(info["func"], info["tableName"]):
            df = func(codes,
                      start=kwargs.get('start', '2015-01-01'),
                      end=kwargs.get('end',None)
                      )
            if df is not None:
                Save_and_Log(df, tableName=tableName, **kwargs)

def Save_and_Log(df:pd.DataFrame,
                 tableName:str,
                 date_column="tradeDate",
                 ticker_column="codes",
                 **kwargs
                 ):
    current_dir = os.path.abspath(os.path.dirname(__file__))
    emTableJson = os.path.join(current_dir, '../../dev_files/emData_tableInfo.json')
    rewrite = kwargs.get('rewrite', True)
    save_gm_data_Y(df, date_column, tableName, dataBase_root_path=dataBase_root_path_EM_data, reWrite=rewrite)
    with open(emTableJson, 'r') as f:
        jsonDict = json.load(f, object_pairs_hook=OrderedDict)
    jsonDict[tableName] = {
        "assets": "emData",
        "description": "",
        "date_column": date_column,
        "ticker_column": ticker_column
    }
    with open(emTableJson, "w") as f:
        json.dump(jsonDict, f, indent=4, separators=(",", ": "))

def load_json(path: str):
    with open(path, 'r') as fp:
        return json.load(fp, object_pairs_hook=OrderedDict)

def check_status(data):
    if (data.ErrorCode != 0):
        print(data.ErrorMsg)
        return False
    else:
        return True

def get_tradeDate_range(
        Date:str,
        offset:str):
    if offset == 0:
        datelst = [Date]
    else:
        offset_day = c.getdate(Date, offset, "Market=CNSESH")
        offset_day = offset_day.Data[0]
        datelst = c.tradedates(offset_day, Date, "period=1,order=1,market=CNSESH")
        datelst = datelst.Data
    return datelst