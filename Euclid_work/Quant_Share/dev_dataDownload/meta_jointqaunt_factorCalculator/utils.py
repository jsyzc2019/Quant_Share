from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from Euclid_work.Quant_Share.Utils import dataBase_root_path_JointQuant_Factor, save_data_Y, printJson
from datetime import datetime
from .base_package import *
from tqdm import tqdm
import pandas as pd
import numpy as np

from Euclid_work.Quant_Share import get_data


def quarter_net(stock_file, factor_name):
    stock_file = stock_file.sort_values(['symbol', 'rpt_date']).reset_index(drop=True)
    stock_file[factor_name + "_1"] = stock_file.groupby(['symbol'])[factor_name].shift(1)
    stock_file[factor_name + "_q"] = stock_file[factor_name] - stock_file[factor_name + "_1"]
    stock_file['quarter'] = stock_file['rpt_date'].dt.quarter
    list1 = list(stock_file[stock_file['quarter'] == 1].index)
    stock_file.loc[list1, factor_name + "_q"] = stock_file.loc[list1, factor_name]
    list2 = list(stock_file[stock_file[factor_name + "_q"] == np.nan].index)
    stock_file['a'] = stock_file[factor_name] / stock_file['quarter']
    stock_file.loc[list2, factor_name + "_q"] = stock_file.loc[list2, 'a']
    stock_file[factor_name] = stock_file[factor_name + "_q"]
    stock_file.drop([factor_name + "_1", factor_name + "_q", 'quarter'], axis=1, inplace=True)
    return stock_file


# 补全数据中的缺失季度
def fill_quarter(stock_file, factor_list=None):
    if factor_list is None:
        factor_list = []
    stock_unique = np.unique(stock_file['symbol'])
    res = pd.DataFrame()
    for i in stock_unique:
        df = stock_file[stock_file['symbol'] == i]
        df['quarter'] = pd.to_datetime(df['rpt_date']).dt.to_period('Q')
        df = df.sort_values(['rpt_date'])
        df = df.drop_duplicates(subset=['quarter'], keep='last')
        df = df.set_index('quarter').resample('Q').asfreq().reset_index()
        df['symbol'] = df['symbol'].ffill()
        for m in range(len(df.index)):
            if pd.isnull(df.loc[m, "rpt_date"]):
                df.loc[m, "rpt_date"] = df['quarter'][m].to_timestamp() + pd.offsets.MonthEnd(3)
        res = pd.concat([df, res], axis=0)
    for j in factor_list:
        res = quarter_net(res, j)
    res = res.reset_index(drop=True)
    return res


def useful_field(df, column_name):
    """
    查找某一列中所字符串
    :param df:
    :param column_name:
    :return:
    """
    str_lst = df[column_name]
    res = []
    for i in str_lst:
        if type(i) == str:
            i = i.replace(" ", '')
            tmp = i.split(',')
            res += tmp
    res = list(set(res))
    if "" in res:
        res.remove('')
    res = ['symbol', 'rpt_date', 'pub_date'] + res
    return res

def to_datatime(df, time_cols=None, format_str='%Y-%m-%d'):
    if time_cols is None:
        time_cols = ['rpt_date', 'pub_date']
    for col in time_cols:
        if format_str != '':
            df[col] = df[col].dt.strftime(format_str)
        df[col] = pd.to_datetime(df[col])
    return df

def deal(stock_file, i):
    df = stock_file[stock_file['symbol'] == i].copy()
    df['pub_date'] = pd.to_datetime(df['pub_date']).dt.to_period('D')
    df = df.set_index('pub_date').resample('D').asfreq().reset_index().ffill()
    return df

def run_thread(stock_file, stock_unique, func, max_work_count):
    # dfs = [stock_file[stock_file['symbol'] == i].copy() for i in stock_unique]
    with ThreadPoolExecutor(max_workers=max_work_count) as t:
        res = [t.submit(func, stock_file, i) for i in stock_unique]
        return res

# 将数据转换为日度数据
def change_frequency(stock_file, Mean_day_list):
    stock_file = stock_file.copy()
    stock_unique = np.unique(stock_file['symbol'])
    stock_file[Mean_day_list] = stock_file[Mean_day_list] / 63
    stock_file = stock_file.sort_values(by=['symbol', 'pub_date', 'rpt_date'])
    stock_file.drop_duplicates(subset=['symbol', 'pub_date'], keep='last', inplace=True)
    res = pd.DataFrame()

    # tmp_res = run_thread(stock_file, stock_unique, deal, 20)
    # for df in as_completed(tmp_res):
    #     tmp = df.result()
    #     res = pd.concat([tmp, res], axis=0)

    for i in tqdm(stock_unique, mininterval=10, leave=False):
        df = stock_file[stock_file['symbol'] == i]
        df['pub_date'] = pd.to_datetime(df['pub_date']).dt.to_period('D')
        df = df.set_index('pub_date').resample('D').asfreq().reset_index().ffill()
        res = pd.concat([df, res], axis=0)
    res = res.reset_index(drop=True)
    res['pub_date'] = res["pub_date"].dt.to_timestamp()
    res.drop(['rpt_date'], axis=1, inplace=True)
    return res

# 获取因子需要的数据
def choose_data(factor_name, joint_quant_factor):
    df = joint_quant_factor.copy()
    df = df[df['factor_name'] == factor_name].reset_index(drop=True).T
    if pd.isnull(df.loc['pre', 0]):
        if not pd.isnull(df.loc['balance_sheet', 0]) or not pd.isnull(df.loc['cashflow_sheet', 0]) \
                or not pd.isnull(df.loc['income_sheet', 0]) or not pd.isnull(df.loc['deriv_finance_indicator', 0]) \
                or not pd.isnull(df.loc['balance_old', 0]):
            if pd.isnull(df.loc['market_sheet', 0]) and pd.isnull(df.loc['trading_derivative_indicator', 0]):
                return 'financial_sheet'
            else:
                return 'market_financial_sheet'
        else:
            return 'market_sheet'
    else:
        return 'ResConSecCorederi_sheet'

# 获取储存因子数据中的最大值
def factor_max_rpt(factor_name):
    folder_path = os.path.join(dataBase_root_path_JointQuant_Factor, factor_name)
    file_name_list = os.listdir(folder_path)
    file_name_list = sorted(file_name_list)
    file_path = os.path.join(folder_path, file_name_list[-1])
    df = pd.read_hdf(file_path)
    max_rpt = df['rpt_date'].max()
    if pd.isnull(max_rpt):
        year = "".join(list(filter(str.isdigit, file_name_list[-1])))
        year = year[:4]
        max_rpt = year + '-01-01'
        max_rpt = datetime.strptime(max_rpt, '%Y-%m-%d')
    return max_rpt

def update(func, factor_name, joint_quant_factor, data_prepare):
    Wrong = {}
    try:
        file_path = os.path.join(dataBase_root_path_JointQuant_Factor, factor_name)
        data_name = choose_data(factor_name, joint_quant_factor)
        df = get_data(data_name,
                      begin=data_prepare.beginDate,
                      end=data_prepare.endDate
                      )
        if os.path.exists(file_path):
            max_rpt = factor_max_rpt(file_path)

            max_rpt1 = df['rpt_date'].max()
            if max_rpt >= max_rpt1:
                print('根据quant_share中的数据，' + factor_name + '因子不需要更新')
                return Wrong
            else:
                if 'close' in df.columns:
                    start_date = str(max_rpt.date().year - 3) + '-01-01'
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                    df = df[df['rpt_date'] >= start_date]
        # func = globals()[factor_name]
        # if 'pub_date' not in df.columns:
        #     df['pub_date'] = df['rpt_date']
        factor = func(df)
        save_data_Y(df=factor, date_column_name='rpt_date', tableName=factor_name,
                    _dataBase_root_path=dataBase_root_path_JointQuant_Factor, reWrite=True)
        print(factor_name + '更新完毕')
        return Wrong
    except:
        Wrong[factor_name] = traceback.format_exc()
        return Wrong
