import os
from ..Utils import dataBase_root_path_JointQuant_Factor
from ..EuclidGetData import get_data
from .JointQuant_utils import *
from datetime import datetime
from gm.api import *

def gm_log():
    set_token('cac6f11ecf01f9539af72142faf5c3066cb1915b')

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
def fill_quarter(stock_file, factor_list=[]):
    stock_unique = np.unique(list(stock_file['symbol']))
    df1 = pd.DataFrame()
    for i in stock_unique:
        df = stock_file[stock_file['symbol'] == i].copy()
        df['quarter'] = pd.to_datetime(df['rpt_date']).dt.to_period('Q')
        df = df.sort_values(['rpt_date'])
        df.drop_duplicates(subset=['quarter'], keep='last', inplace=True)
        df = df.set_index('quarter').resample('Q').asfreq().reset_index()
        df['symbol'] = df['symbol'].ffill()
        for m in range(len(df.index)):
            if pd.isnull(df.loc[m, "rpt_date"]):
                df.loc[m, "rpt_date"] = df['quarter'][m].to_timestamp() + pd.offsets.MonthEnd(3)
        df1 = pd.concat([df, df1], axis=0)
    for j in factor_list:
        df1 = quarter_net(df1, j)
    df1 = df1.reset_index(drop=True)
    return df1


# 查找某一列中所字符串
def f(df, column_name):
    b_list = df[column_name]
    c = []
    for i in b_list:
        if type(i) == str:
            c = c + i.split(',')
    while '' in c:
        c.remove('')
    c = list(set(c))
    c = ['symbol', 'rpt_date', 'pub_date'] + c
    return c


# 将数据转换为日度数据
def change_frequency(stock_file, Mean_day_list):
    stock_file = stock_file.copy()
    stock_unique = np.unique(list(stock_file['symbol']))
    stock_file[Mean_day_list] = stock_file[Mean_day_list] / 63
    stock_file = stock_file.sort_values(by=['symbol', 'pub_date', 'rpt_date'])
    stock_file.drop_duplicates(subset=['symbol', 'pub_date'], keep='last', inplace=True)
    df1 = pd.DataFrame()
    for i in stock_unique:
        df = stock_file[stock_file['symbol'] == i].copy()
        df['pub_date'] = pd.to_datetime(df['pub_date']).dt.to_period('D')
        df = df.set_index('pub_date').resample('D').asfreq().reset_index().ffill()
        df1 = pd.concat([df, df1], axis=0)
    df1 = df1.reset_index(drop=True)
    df1['pub_date'] = df1["pub_date"].dt.to_timestamp()
    df1.drop(['rpt_date'], axis=1, inplace=True)
    return df1


def original_data():
    '''
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
    '''

    balance_sheet = get_data('fundamentals_balance')
    balance_sheet1 = get_data('balance_sheet')
    income_sheet = get_data('fundamentals_income')
    cashflow_sheet = get_data('fundamentals_cashflow')
    deriv_finance_sheet = get_data('deriv_finance_indicator')
    deriv_finance_sheet[deriv_finance_sheet == 0] = np.nan
    balance_sheet1[balance_sheet1 == 0] = np.nan

    market_sheet = get_data('gmData_history')  # 更改日期格式，并且改名字为rpt_date
    share_number_sheet = get_data('share_change')  # chg_date变动日期 pub_date发布日期 取大的作为rpt_date
    market_sheet2 = get_data('trading_derivative_indicator')
    market_sheet2[market_sheet2 == 0] = np.nan

    balance_sheet.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last', inplace=True)
    balance_sheet1.drop_duplicates(subset=['symbol', 'end_date'], keep='last', inplace=True)
    income_sheet.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last', inplace=True)
    cashflow_sheet.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last', inplace=True)
    deriv_finance_sheet.drop_duplicates(subset=['symbol', 'end_date'], keep='last', inplace=True)
    market_sheet.drop_duplicates(subset=['symbol', 'eob'], keep='last', inplace=True)
    market_sheet2.drop_duplicates(subset=['symbol', 'end_date'], keep='last', inplace=True)

    balance_sheet = balance_sheet.reset_index(drop=True)
    balance_sheet1 = balance_sheet1.reset_index(drop=True)
    income_sheet = income_sheet.reset_index(drop=True)
    cashflow_sheet = cashflow_sheet.reset_index(drop=True)
    deriv_finance_sheet = deriv_finance_sheet.reset_index(drop=True)
    market_sheet = market_sheet.reset_index(drop=True)
    market_sheet2 = market_sheet2.reset_index(drop=True)

    # 查看需要的值
    df = pd.read_excel(os.path.join(os.path.dirname(__file__), '../dev_files/jointquant_factor.xlsx'))
    b_name = f(df, 'balance_sheet')
    b_old_name = f(df, 'balance_old')
    i_name = f(df, 'income_sheet')
    c_name = f(df, 'cashflow_sheet')
    d_name = f(df, 'deriv_finance_indicator')

    # 资产负债表填充缺失值并改变日期格式
    balance_sheet = balance_sheet[b_name].copy()
    balance_sheet["rpt_date"] = pd.to_datetime(balance_sheet['rpt_date'])
    balance_sheet["pub_date"] = pd.to_datetime(balance_sheet['pub_date'])

    # 原始接口资产负债表数据
    b_old_name.remove('rpt_date')
    b_old_name.append('end_date')
    balance_sheet1 = balance_sheet1[b_old_name].copy()
    balance_sheet1.rename(columns={'end_date': 'rpt_date'}, inplace=True)
    balance_sheet1['rpt_date'] = balance_sheet1['rpt_date'].dt.strftime('%Y-%m-%d')
    balance_sheet1["rpt_date"] = pd.to_datetime(balance_sheet1['rpt_date'])
    balance_sheet1['pub_date'] = balance_sheet1['pub_date'].dt.strftime('%Y-%m-%d')
    balance_sheet1["pub_date"] = pd.to_datetime(balance_sheet1['pub_date'])
    balance_sheet = pd.merge(balance_sheet, balance_sheet1, on=['symbol', 'rpt_date'], how='outer')
    balance_sheet['pub_date'] = balance_sheet[['pub_date_x', 'pub_date_y']].max(axis=1)
    balance_sheet.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)

    # 利润表填充缺失值并改变日期格式并改为非累计
    income_sheet = income_sheet[i_name].copy()
    income_sheet["rpt_date"] = pd.to_datetime(income_sheet['rpt_date'])
    income_sheet["pub_date"] = pd.to_datetime(income_sheet['pub_date'])
    change_list = list(income_sheet.columns)
    change_list.remove('symbol')
    change_list.remove('pub_date')
    change_list.remove('rpt_date')
    income_sheet = fill_quarter(income_sheet, change_list)

    # 现金流量表填充缺失值并改变日期格式并改为非累计
    cashflow_sheet = cashflow_sheet[c_name].copy()
    cashflow_sheet["rpt_date"] = pd.to_datetime(cashflow_sheet['rpt_date'])
    cashflow_sheet["pub_date"] = pd.to_datetime(cashflow_sheet['pub_date'])
    change_list = list(cashflow_sheet.columns)
    change_list.remove('symbol')
    change_list.remove('pub_date')
    change_list.remove('rpt_date')
    cashflow_sheet = fill_quarter(cashflow_sheet, change_list)

    # 衍生表
    d_name.remove('rpt_date')
    d_name.append('end_date')
    deriv_finance_sheet = deriv_finance_sheet[d_name].copy()
    deriv_finance_sheet.rename(columns={'end_date': 'rpt_date'}, inplace=True)
    deriv_finance_sheet['rpt_date'] = deriv_finance_sheet['rpt_date'].dt.strftime('%Y-%m-%d')
    deriv_finance_sheet["rpt_date"] = pd.to_datetime(deriv_finance_sheet['rpt_date'])
    deriv_finance_sheet['pub_date'] = deriv_finance_sheet['pub_date'].dt.strftime('%Y-%m-%d')
    deriv_finance_sheet["pub_date"] = pd.to_datetime(deriv_finance_sheet['pub_date'])

    # 财务报表进行合并
    financial_data = pd.merge(income_sheet, cashflow_sheet, on=['symbol', 'rpt_date'], how='outer')
    financial_data = pd.merge(financial_data, balance_sheet, on=['symbol', 'rpt_date'], how='outer')
    financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y', 'pub_date']].max(axis=1)
    financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)
    financial_data = pd.merge(financial_data, deriv_finance_sheet, on=['symbol', 'rpt_date'], how='outer')
    financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y']].max(axis=1)
    financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1, inplace=True)

    # 股本数据进行日度填充
    share_number_sheet = get_data('share_change')
    share_number_sheet["pub_date"] = pd.to_datetime(share_number_sheet['pub_date'])
    share_number_sheet["chg_date"] = pd.to_datetime(share_number_sheet['chg_date'])
    share_number_sheet['rpt_date'] = share_number_sheet[['pub_date', 'chg_date']].max(axis=1)
    share_number_sheet = share_number_sheet.sort_values(['symbol', 'rpt_date', 'pub_date'])
    share_number_sheet = share_number_sheet.drop_duplicates(['symbol', 'rpt_date'], keep='last')
    share_number_sheet = share_number_sheet[['symbol', 'rpt_date', 'share_total', 'share_circ']]
    stock_unique = np.unique(list(share_number_sheet['symbol']))
    share_number2 = pd.DataFrame()
    for i in stock_unique:
        share_number = share_number_sheet[share_number_sheet['symbol'] == i].copy()
        if str(share_number['rpt_date'].max()) < str(datetime.now().date()):
            a = pd.DataFrame({'rpt_date': [str(datetime.now().date())]})
            share_number = pd.concat([a, share_number], axis=0)
        share_number['rpt_date'] = pd.to_datetime(share_number['rpt_date']).dt.to_period('D')
        share_number = share_number.set_index('rpt_date').resample('D').asfreq().reset_index().ffill()
        share_number2 = pd.concat([share_number, share_number2], axis=0)
    share_number_sheet = share_number2.reset_index(drop=True)
    share_number_sheet["rpt_date"] = share_number_sheet["rpt_date"].dt.to_timestamp()

    financial_data = pd.merge(financial_data, share_number_sheet, on=['symbol', 'rpt_date'], how='left')
    financial_data.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last', inplace=True)

    # 市场数据改变日期格式
    market_sheet['rpt_date'] = market_sheet['eob'].dt.strftime('%Y-%m-%d')
    market_sheet["rpt_date"] = pd.to_datetime(market_sheet['rpt_date'])
    market_sheet.drop(['bob', 'position', 'eob'], axis=1, inplace=True)
    market_sheet1 = history(symbol='SHSE.000300', frequency='1d', start_time='2015-01-01', end_time='2023-05-29',
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
    market_sheet2 = market_sheet2[['symbol', 'rpt_date', 'PCTTM', 'PSTTM', 'PELFY', 'NEGOTIABLEMV', 'TOTMKTCAP']]

    market_sheet = pd.merge(market_sheet, market_sheet2, on=['symbol', 'rpt_date'], how='left')
    market_sheet['pub_date'] = market_sheet['rpt_date']

    # 市场数据和财务数据进行合并
    list1 = i_name[3:] + c_name[3:]
    financial_data1 = change_frequency(financial_data, list1)
    market_financial_sheet = pd.merge(market_sheet, financial_data1, on=['symbol', 'pub_date'], how='left')
    market_financial_sheet['rpt_date'] = market_sheet['pub_date']
    market_financial_sheet['share_total'] = market_financial_sheet['share_total_x']
    market_financial_sheet.drop(['share_total_x', 'share_total_y'], axis=1, inplace=True)
    market_financial_sheet['share_circ'] = market_financial_sheet['share_circ_x']
    market_financial_sheet.drop(['share_circ_x', 'share_circ_y'], axis=1, inplace=True)

    # 预期数据-日度
    ResConSecCorederi = get_data('ResConSecCorederi')
    ResConSecCorederi = ResConSecCorederi[['secCode', 'repForeDate', 'foreYear', 'updateTime', 'conPe', 'conProfitYoy']]
    ResConSecCorederi["repForeDate"] = pd.to_datetime(ResConSecCorederi['repForeDate'])
    ResConSecCorederi.rename(columns={'repForeDate': 'rpt_date'}, inplace=True)
    ResConSecCorederi.rename(columns={'secCode': 'ticker'}, inplace=True)
    ResConSecCorederi['year'] = ResConSecCorederi["rpt_date"].dt.year
    ResConSecCorederi['a'] = ResConSecCorederi['foreYear'] - ResConSecCorederi['year']
    ResConSecCorederi = ResConSecCorederi[ResConSecCorederi['a'] == 1]
    ResConSecCorederi = ResConSecCorederi.reset_index(drop=True)
    ResConSecCorederi["updateTime"] = pd.to_datetime(ResConSecCorederi['updateTime'])
    ResConSecCorederi['updateTime'] = ResConSecCorederi['updateTime'].dt.strftime('%Y-%m-%d')
    ResConSecCorederi["updateTime"] = pd.to_datetime(ResConSecCorederi['updateTime'])
    ResConSecCorederi.drop_duplicates(subset=['ticker', 'rpt_date', 'foreYear'], keep='last', inplace=True)
    ResConSecCorederi = ResConSecCorederi.reset_index(drop=True)

    pre_data = market_sheet2.copy()
    pre_data['ticker'] = [x[-6:] for x in pre_data['symbol']]
    ResConSecCorederi = pd.merge(pre_data, ResConSecCorederi, on=['ticker', 'rpt_date'], how='outer')
    ResConSecCorederi.drop(['symbol', 'year', 'a'], axis=1, inplace=True)
    ResConSecCorederi.rename(columns={'ticker': 'symbol'}, inplace=True)
    ResConSecCorederi.rename(columns={'updateTime': 'pub_date'}, inplace=True)

    return financial_data, market_sheet, market_financial_sheet, ResConSecCorederi


# 获取因子需要的数据
def data(factor_name, financial_data, market_sheet, market_financial_sheet, ResConSecCorederi):
    df = pd.read_excel(os.path.join(os.path.dirname(__file__), "../dev_files/jointquant_factor.xlsx"))
    df = df[df['factor_name'] == factor_name].reset_index(drop=True).T
    if pd.isnull(df.loc['pre', 0]):
        if not pd.isnull(df.loc['balance_sheet', 0]) or not pd.isnull(df.loc['cashflow_sheet', 0]) \
                or not pd.isnull(df.loc['income_sheet', 0]) or not pd.isnull(df.loc['deriv_finance_indicator', 0]) \
                or not pd.isnull(df.loc['balance_old', 0]):
            if pd.isnull(df.loc['market_sheet', 0]) and pd.isnull(df.loc['trading_derivative_indicator', 0]):
                return financial_data
            else:
                return market_financial_sheet
        else:
            return market_sheet
    else:
        return ResConSecCorederi


# 保存数据
# date_column_name时间列名
# save_file_name储存文件名
def save_data(df, date_column_name, save_file_name):
    df = df.copy()
    df["year"] = df[date_column_name].dt.year
    # 储存数据
    # 创建因子文件夹
    file_path = os.path.join(dataBase_root_path_JointQuant_Factor, save_file_name)
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
    for i in range(df["year"].min(), df["year"].max() + 1):
        file_path1 = os.path.join(file_path, save_file_name + '_Y' + str(i) + '.h5')
        df1 = df[df["year"] == i]
        df1 = df1.dropna(axis=0)
        df1 = df1.drop(['year'], axis=1)
        df1 = df1.sort_values(['symbol', date_column_name])
        df1 = df1.reset_index(drop=True)
        df1.to_hdf(file_path1, key="a", mode='a')


# 获取储存因子数据中的最大值
def factor_max_rpt(factor_name):
    file_path = os.path.join(dataBase_root_path_JointQuant_Factor, factor_name)
    file_name_list = os.listdir(file_path)
    file_name_list = sorted(file_name_list)
    file_path1 = os.path.join(file_path, file_name_list[-1])
    df = pd.read_hdf(file_path1)
    max_rpt = df['rpt_date'].max()
    if pd.isnull(max_rpt):
        year = "".join(list(filter(str.isdigit, file_name_list[-1])))
        year = year[:4]
        max_rpt = year + '-01-01'
        max_rpt = datetime.strptime(max_rpt, '%Y-%m-%d')
    return max_rpt


def renew():
    df = pd.read_excel(os.path.join(os.path.dirname(__file__), '../dev_files/jointquant_factor.xlsx'))
    factor_list = list(df['factor_name'])
    financial_data, market_sheet, market_financial_sheet, ResConSecCorederi = original_data()
    for i in factor_list:
        file_path = os.path.join(dataBase_root_path_JointQuant_Factor, i)
        if os.path.exists(file_path):
            max_rpt = factor_max_rpt(i)
            df = data(i, financial_data, market_sheet, market_financial_sheet, ResConSecCorederi)
            max_rpt1 = df['rpt_date'].max()
            if max_rpt >= max_rpt1:
                print('根据quant_share中的数据，' + i + '因子不需要更新')
            else:
                if 'close' in df.columns:
                    start_date = str(int(str(max_rpt.date())[:4]) - 3) + '-01-01'
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                    df = df[df['rpt_date'] >= start_date]
                    df = df.copy()
                    factor = globals()[i](df)
                    start_date1 = str(int(str(max_rpt.date())[:4])) + '-01-01'
                    start_date1 = datetime.strptime(start_date1, '%Y-%m-%d')
                    factor = factor[factor['rpt_date'] >= start_date1]
                    save_data(factor, 'rpt_date', i)
                    print(i + '因子更新完毕')
                else:
                    factor = globals()[i](df)
                    start_date1 = str(int(str(max_rpt.date())[:4])) + '-01-01'
                    start_date1 = datetime.strptime(start_date1, '%Y-%m-%d')
                    factor = factor[factor['rpt_date'] >= start_date1]
                    save_data(factor, 'rpt_date', i)
                    print(i + '因子更新完毕')
        else:
            df = data(i, financial_data, market_sheet, market_financial_sheet, ResConSecCorederi)
            df = df.copy()
            factor = globals()[i](df)
            save_data(factor, 'rpt_date', i)
            print(i + '因子更新完毕')
