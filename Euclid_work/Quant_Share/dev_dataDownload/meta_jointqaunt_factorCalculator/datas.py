import pandas as pd
import numpy as np
from Euclid_work.Quant_Share import get_data, get_tradeDate
import os
from tqdm import tqdm
from .utils import useful_field, fill_quarter, change_frequency, to_datatime
from .base_package import *
from datetime import datetime

__all__ = ['GetOringinalData', 'get_joint_quant_factor']

def get_joint_quant_factor() -> pd.DataFrame:
    return pd.read_excel(os.path.join(os.path.dirname(__file__), "../../dev_files/jointquant_factor.xlsx"))

def GetOringinalData(beginDate:str=None, **kwargs):
    """
    读入数据-日期全部为datetime格式
    财务数据：需要把数据补齐并且现金流量表和利润表计算为季度值（非累计值）
    股本数据：fill
    将股本数据和指数数据加入到market中
    start_date=datetime.strptime(start_date,'%Y-%m-%d')
    end_date=datetime.strptime(end_date,'%Y-%m-%d')
    资产负债表 fundamentals_balance
    现金流量表 fundamentals_cashflow
    利润表 fundamentals_income
    市场数据 gmData_history
    股本数据 share_change
    """

    if beginDate is None:
        beginDate = get_tradeDate(datetime.today(), -365*5)


    filenames = ['fundamentals_balance', 'balance_sheet', 'fundamentals_income',
                 'fundamentals_cashflow', 'deriv_finance_indicator', 'gmData_history', 'trading_derivative_indicator',
                 'share_change', 'financial_data', 'market_sheet', 'ResConSecCorederi', 'market_financial_sheet']

    replace_lst = ['deriv_finance_indicator', 'balance_sheet', 'trading_derivative_indicator']


    datas = {}
    jointquant_factor = get_joint_quant_factor()
    with tqdm(filenames, leave=True) as t:
        for file in t:
            t.set_description(f"处理{file}中...")
            if file == 'financial_data':
                financial_process(datas)
                continue
            elif file == 'market_sheet':
                market_sheet_process(datas)
                continue
            elif file == 'market_financial_sheet':
                market_financial_process(jointquant_factor, datas)
                continue

            df = get_data(file, begin=beginDate)

            if file not in ['share_change', 'ResConSecCorederi']:
                df = df.reset_index(drop=True)

            if file in replace_lst:
                df[df == 0] = np.nan
            if file in ['fundamentals_balance', 'fundamentals_income', 'fundamentals_cashflow']:
                df = df.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last')
            elif file in ['balance_sheet', 'deriv_finance_indicator', 'trading_derivative_indicator']:
                df = df.drop_duplicates(subset=['symbol', 'end_date'], keep='last')
            elif file in ['gmData_history']:
                df = df.drop_duplicates(subset=['symbol', 'eob'], keep='last')

            if file == 'fundamentals_balance':
                name = useful_field(jointquant_factor, 'balance_sheet')
                df = df[name]
                df = to_datatime(df, format_str='')
                datas['balance_sheet'] = {'df': df, 'file': file}
            elif file == 'fundamentals_cashflow':
                # 现金流量表填充缺失值并改变日期格式并改为非累计
                fundamentals_process(df, jointquant_factor, 'cashflow_sheet', datas)
            elif file == 'fundamentals_income':
                # 利润表填充缺失值并改变日期格式并改为非累计
                fundamentals_process(df, jointquant_factor, 'income_sheet', datas)
            elif file == 'deriv_finance_indicator':
                name = useful_field(jointquant_factor, 'deriv_finance_indicator')
                name.remove('rpt_date')
                name.append('end_date')
                df = df[name]
                df.rename(columns={'end_date': 'rpt_date'}, inplace=True)
                df = to_datatime(df)
                datas[file] = {'df': df, 'file': file}
            elif file == 'balance_sheet':
                # 原始接口资产负债表数据
                name = useful_field(jointquant_factor, 'balance_old')
                name.remove('rpt_date')
                name.append('end_date')
                df = df[name]
                df.rename(columns={'end_date': 'rpt_date'}, inplace=True)
                df = to_datatime(df)
                df = pd.merge(datas['balance_sheet']['df'], df, on=['symbol', 'rpt_date'], how='outer')
                df['pub_date'] = df[['pub_date_x', 'pub_date_y']].max(axis=1)
                df.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)
                datas['balance_sheet']['df'] = df
                assert 'ttl_ast' in df.columns
            elif file == 'gmData_history':
                datas['market_sheet'] = {'df': df}
            elif file == 'trading_derivative_indicator':
                datas['market_sheet2'] = {'df': df}
            elif file == 'share_change':
                # continue
                # 股本数据进行日度填充
                share_change_process(df, datas)
            elif file == 'ResConSecCorederi':
                ResConSecCorederi_process(df, datas)

    res = ['financial_data', 'market_sheet', 'market_financial_sheet', 'ResConSecCorederi']
    return [datas[i]['df'] for i in res]

def financial_process(datas: dict):
    # 财务报表进行合并
    financial_data = pd.merge(datas['income_sheet']['df'], datas['cashflow_sheet']['df'],
                              on=['symbol', 'rpt_date'], how='outer')
    financial_data = pd.merge(financial_data, datas['balance_sheet']['df'], on=['symbol', 'rpt_date'], how='outer')
    financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y', 'pub_date']].max(axis=1)
    financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)
    financial_data = pd.merge(financial_data, datas['deriv_finance_indicator']['df'], on=['symbol', 'rpt_date'],
                              how='outer')
    financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y']].max(axis=1)
    financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)
    financial_data = pd.merge(financial_data, datas['share_number_sheet']['df'], on=['symbol', 'rpt_date'], how='left')
    financial_data.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last', inplace=True)
    datas['financial_data'] = {'df': financial_data}

def ResConSecCorederi_process(df:pd.DataFrame, datas:dict):
    # 预期数据-日度
    df = df[
        ['secCode', 'repForeDate', 'foreYear', 'updateTime', 'conPe', 'conProfitYoy']]
    df["repForeDate"] = pd.to_datetime(df['repForeDate'])
    df.rename(columns={'repForeDate': 'rpt_date', 'secCode': 'ticker'}, inplace=True)
    df['year'] = df["rpt_date"].dt.year
    df['a'] = df['foreYear'] - df['year']
    df = df[df['a'] == 1]
    df = df.reset_index(drop=True)
    df["updateTime"] = pd.to_datetime(df['updateTime'])
    df['updateTime'] = df['updateTime'].dt.strftime('%Y-%m-%d')
    df["updateTime"] = pd.to_datetime(df['updateTime'])
    df.drop_duplicates(subset=['ticker', 'rpt_date', 'foreYear'], keep='last', inplace=True)
    df = df.reset_index(drop=True)
    pre_data = datas['market_sheet2']['df'].copy()
    pre_data['ticker'] = [x[-6:] for x in pre_data['symbol']]
    df = pd.merge(pre_data, df, on=['ticker', 'rpt_date'], how='outer')
    df.drop(['symbol', 'year', 'a'], axis=1, inplace=True)
    df.rename(columns={'ticker': 'symbol', 'updateTime': 'pub_date'}, inplace=True)
    datas['ResConSecCorederi'] = {'df':df, 'file':'ResConSecCorederi'}

def fundamentals_process(df, jointquant_factor, file, datas):
    name = useful_field(jointquant_factor, file)
    df = df[name].copy()
    df = to_datatime(df, format_str='')
    change_list = list(df.columns)
    change_list.remove('symbol')
    change_list.remove('pub_date')
    change_list.remove('rpt_date')
    df = fill_quarter(df, change_list)
    datas[file] = {'df': df, 'file': file}

def share_change_process(df, datas):
    df = to_datatime(df, time_cols=['pub_date', 'chg_date'], format_str='')
    df['rpt_date'] = df[['pub_date', 'chg_date']].max(axis=1)
    df = df.sort_values(['symbol', 'rpt_date', 'pub_date'])
    df = df.drop_duplicates(['symbol', 'rpt_date'], keep='last')
    df = df[['symbol', 'rpt_date', 'share_total', 'share_circ']]
    stock_unique = np.unique(df['symbol'])
    res = pd.DataFrame()
    for stock in stock_unique[:10]:
        share_number = df[df['symbol'] == stock].copy()
        if share_number['rpt_date'].max() < datetime.now().date():
            a = pd.DataFrame({'rpt_date': [str(datetime.now().date())]})
            share_number = pd.concat([a, share_number], axis=0)
        share_number['rpt_date'] = pd.to_datetime(share_number['rpt_date']).dt.to_period('D')
        share_number = share_number.set_index('rpt_date').resample('D').asfreq().reset_index().ffill()
        res = pd.concat([share_number, res], axis=0)
    share_number_sheet = res.reset_index(drop=True)
    share_number_sheet["rpt_date"] = share_number_sheet["rpt_date"].dt.to_timestamp()
    datas['share_number_sheet'] = {'df': share_number_sheet, 'file': 'share_change'}

def market_sheet_process(datas):
    # 市场数据改变日期格式
    market_sheet = datas['market_sheet']['df']
    market_sheet2 = datas['market_sheet2']['df'].copy()
    share_number_sheet = datas['share_number_sheet']['df']
    market_sheet['rpt_date'] = market_sheet['eob'].dt.strftime('%Y-%m-%d')
    market_sheet["rpt_date"] = pd.to_datetime(market_sheet['rpt_date'])
    market_sheet.drop(['bob', 'position', 'eob'], axis=1, inplace=True)
    market_sheet1 = history(symbol='SHSE.000300', frequency='1d', start_time='2015-01-01',
                            end_time='2023-05-29',
                            fields=' close,eob,pre_close', df=True)
    market_sheet1['rpt_date'] = market_sheet1['eob'].dt.strftime('%Y-%m-%d')
    market_sheet1["rpt_date"] = pd.to_datetime(market_sheet1['rpt_date'])
    market_sheet1.drop(['eob'], axis=1, inplace=True)
    market_sheet1 = market_sheet1.reset_index(drop=True)
    market_sheet1.rename(columns={'close': 'index_close', 'pre_close': 'pre_index_close'}, inplace=True)
    market_sheet = pd.merge(market_sheet, market_sheet1, on=['rpt_date'], how='left')
    market_sheet = pd.merge(market_sheet, share_number_sheet, on=['symbol', 'rpt_date'], how='left')
    market_sheet2.rename(columns={'end_date': 'rpt_date'}, inplace=True)
    market_sheet2['rpt_date'] = market_sheet2['rpt_date'].dt.strftime('%Y-%m-%d')
    market_sheet2["rpt_date"] = pd.to_datetime(market_sheet2['rpt_date'])
    market_sheet2 = market_sheet2[
        ['symbol', 'rpt_date', 'PCTTM', 'PSTTM', 'PELFY', 'NEGOTIABLEMV', 'TOTMKTCAP']]
    market_sheet = pd.merge(market_sheet, market_sheet2, on=['symbol', 'rpt_date'], how='left')
    market_sheet['pub_date'] = market_sheet['rpt_date']
    datas['market_sheet']['df'] = market_sheet
    datas['market_sheet2']['df'] = market_sheet2

def market_financial_process(jointquant_factor, datas):
    # 市场数据和财务数据进行合并
    financial_data = datas['financial_data']['df']
    market_sheet = datas['market_sheet']['df']
    financial_field = useful_field(jointquant_factor, 'income_sheet')[3:] + useful_field(jointquant_factor,
                                                                                         'cashflow_sheet')[3:]
    financial_data1 = change_frequency(financial_data, financial_field)
    market_financial_sheet = pd.merge(market_sheet, financial_data1, on=['symbol', 'pub_date'], how='left')
    market_financial_sheet['rpt_date'] = market_sheet['pub_date']
    market_financial_sheet['share_total'] = market_financial_sheet['share_total_x']
    market_financial_sheet.drop(['share_total_x', 'share_total_y'], axis=1, inplace=True)
    market_financial_sheet['share_circ'] = market_financial_sheet['share_circ_x']
    market_financial_sheet.drop(['share_circ_x', 'share_circ_y'], axis=1, inplace=True)
    datas['market_financial_sheet'] = {'df':market_financial_sheet}

