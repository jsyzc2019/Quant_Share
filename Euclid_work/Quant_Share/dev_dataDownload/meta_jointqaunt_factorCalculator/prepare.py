import os
import pandas as pd
import numpy as np
from Euclid_work.Quant_Share import get_data, get_tradeDate, time_decorator, save_data_Y, \
    dataBase_root_path_JointQuant_prepare, save_data_h5
from Euclid_work.Quant_Share import save_data_Q
from tqdm import tqdm
from .utils import useful_field, fill_quarter, change_frequency, to_datatime
from .base_package import *
from datetime import date, datetime
from functools import cached_property


def get_joint_quant_factor() -> pd.DataFrame:
    return pd.read_excel(os.path.join(os.path.dirname(__file__), "../../dev_files/jointquant_factor.xlsx"))


class DataPrepare():

    def __init__(self, beginDate: str = None, endDate: str = None, **kwargs) -> None:
        self.trading_derivative_indicator_modify = None
        self.beginDate = beginDate if beginDate is None else get_tradeDate(date.today(), -365 * 4)
        self.endDate = endDate if endDate is None else date.today()

    @cached_property
    def joint_quant_factor(self):
        return get_joint_quant_factor()

    def income_cashflow_process(self, df):
        df = df.reset_index(drop=True)
        df = df.sort_values('rpt_date').drop_duplicates(subset=['symbol', 'rpt_date'], keep='last')
        df = to_datatime(df, format_str='')
        change_list = list(df.columns)
        change_list.remove('symbol')
        change_list.remove('pub_date')
        change_list.remove('rpt_date')
        df = fill_quarter(df, change_list)
        return df

    @cached_property
    def income_sheet(self):
        # 利润表填充缺失值并改变日期格式并改为非累计
        df = get_data('fundamentals_income',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=useful_field(self.joint_quant_factor, 'income_sheet'))

        return self.income_cashflow_process(df)

    @cached_property
    def cashflow_sheet(self):
        df = get_data('fundamentals_cashflow',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=useful_field(self.joint_quant_factor, 'cashflow_sheet'))
        return self.income_cashflow_process(df)

    @cached_property
    def fundamentals_balance(self):
        df = get_data('fundamentals_balance',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=useful_field(self.joint_quant_factor, 'balance_sheet'))
        df = df.reset_index(drop=True)
        df = df.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last')
        df = to_datatime(df, format_str='')
        return df

    @cached_property
    def deriv_finance_indicator(self):
        fields = useful_field(self.joint_quant_factor, 'deriv_finance_indicator')
        fields.remove('rpt_date')
        fields.append('end_date')
        df = get_data('deriv_finance_indicator',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=fields)
        df = df.reset_index(drop=True)
        df[df == 0] = np.nan
        df = df.drop_duplicates(subset=['symbol', 'end_date'], keep='last')
        df = df.rename(columns={'end_date': 'rpt_date'})
        df = to_datatime(df)
        return df

    @cached_property
    def gmData_history(self):
        df = get_data('gmData_history',
                      begin=self.beginDate,
                      end=self.endDate,
                      )
        df = df.reset_index(drop=True)
        df = df.drop_duplicates(subset=['symbol', 'eob'], keep='last')
        return df

    @cached_property
    def trading_derivative_indicator(self):
        df = get_data('trading_derivative_indicator',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=['symbol', 'end_date', 'PCTTM', 'PSTTM', 'PELFY', 'NEGOTIABLEMV', 'TOTMKTCAP']
                      )
        df = df.reset_index(drop=True)
        df[df == 0] = np.nan
        df = df.drop_duplicates(subset=['symbol', 'end_date'], keep='last')
        return df

    @cached_property
    def balance_sheet(self):
        # 原始接口资产负债表数据
        fundamentals_balance = self.fundamentals_balance
        fields = useful_field(self.joint_quant_factor, 'balance_old')
        fields.remove('rpt_date')
        fields.append('end_date')
        df = get_data('balance_sheet',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=fields)
        df = df.reset_index(drop=True)
        df[df == 0] = np.nan
        df = df.rename(columns={'end_date': 'rpt_date'})
        df = to_datatime(df)
        df = pd.merge(fundamentals_balance, df, on=['symbol', 'rpt_date'], how='outer')
        df['pub_date'] = df[['pub_date_x', 'pub_date_y']].max(axis=1)
        df = df.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last')
        df['pub_date'] = df[['pub_date_x', 'pub_date_y']].max(axis=1)
        df = df.drop(['pub_date_x', 'pub_date_y'], axis=1)
        return df

    @cached_property
    def share_number_sheet(self):
        df = get_data('share_change',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=['symbol', 'chg_date', 'pub_date', 'share_total', 'share_circ'])
        df = to_datatime(df, time_cols=['pub_date', 'chg_date'], format_str='')
        df['rpt_date'] = df[['pub_date', 'chg_date']].max(axis=1)
        df = df.sort_values(['symbol', 'rpt_date', 'pub_date'])
        df = df.drop_duplicates(['symbol', 'rpt_date'], keep='last')
        df = df.drop(['pub_date'], axis=1)
        stock_unique = np.unique(df['symbol'])
        res = pd.DataFrame()
        for stock in tqdm(stock_unique):
            share_number = df[df['symbol'] == stock].copy()
            if share_number['rpt_date'].max() < datetime.now().date():
                a = pd.DataFrame({'rpt_date': [str(datetime.now().date())]})
                share_number = pd.concat([a, share_number], axis=0)
            share_number['rpt_date'] = pd.to_datetime(share_number['rpt_date']).dt.to_period('D')
            share_number = share_number.set_index('rpt_date').resample('D').asfreq().reset_index().ffill()
            res = pd.concat([share_number, res], axis=0)
        share_number_sheet = res.reset_index(drop=True)
        share_number_sheet["rpt_date"] = share_number_sheet["rpt_date"].dt.to_timestamp()
        return share_number_sheet

    @cached_property
    def financial_sheet(self):
        income_sheet = self.income_sheet
        cashflow_sheet = self.cashflow_sheet
        balance_sheet = self.balance_sheet

        # 财务报表进行合并
        financial_data = pd.merge(income_sheet, cashflow_sheet,
                                  on=['symbol', 'rpt_date'], how='outer')
        financial_data = pd.merge(financial_data, balance_sheet, on=['symbol', 'rpt_date'], how='outer')
        financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y', 'pub_date']].max(axis=1)
        assert 'pub_date' in financial_data.columns
        deriv_finance_indicator = self.deriv_finance_indicator
        share_number_sheet = self.share_number_sheet
        financial_data = financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1)
        financial_data = pd.merge(financial_data, deriv_finance_indicator, on=['symbol', 'rpt_date'],
                                  how='outer')
        financial_data['pub_date'] = financial_data[['pub_date_x', 'pub_date_y']].max(axis=1)
        assert 'pub_date' in financial_data.columns
        financial_data = financial_data.drop(['pub_date_x', 'pub_date_y'], axis=1)
        financial_data = pd.merge(financial_data, share_number_sheet, on=['symbol', 'rpt_date'],
                                  how='left')
        financial_data = financial_data.drop_duplicates(subset=['symbol', 'rpt_date'], keep='last')
        # if 'pub_date' not in financial_data.columns:
        #     financial_data['pub_date'] = financial_data['rpt_date']
        assert 'pub_date' in financial_data.columns
        return financial_data

    @cached_property
    def ResConSecCorederi_sheet(self):
        df = get_data('ResConSecCorederi',
                      begin=self.beginDate,
                      end=self.endDate,
                      fields=['secCode', 'repForeDate', 'foreYear', 'updateTime', 'conPe', 'conProfitYoy']
                      )
        df["repForeDate"] = pd.to_datetime(df['repForeDate'])
        df = df.rename(columns={'repForeDate': 'rpt_date', 'secCode': 'ticker'})
        df['year'] = df["rpt_date"].dt.year
        df['a'] = df['foreYear'] - df['year']
        df = df[df['a'] == 1]
        df = df.reset_index(drop=True)
        df["updateTime"] = pd.to_datetime(df['updateTime'])
        df['updateTime'] = df['updateTime'].dt.strftime('%Y-%m-%d')
        df["updateTime"] = pd.to_datetime(df['updateTime'])
        df = df.drop_duplicates(subset=['ticker', 'rpt_date', 'foreYear'], keep='last')
        df = df.reset_index(drop=True)
        pre_data = self.trading_derivative_indicator_modify
        pre_data['ticker'] = [x[-6:] for x in pre_data['symbol']]
        df = pd.merge(pre_data, df, on=['ticker', 'rpt_date'], how='outer')
        df = df.drop(['symbol', 'year', 'a'], axis=1)
        df = df.rename(columns={'ticker': 'symbol', 'updateTime': 'pub_date'})
        return df

    @cached_property
    def market_sheet(self):
        # 市场数据改变日期格式
        gmData_history = self.gmData_history
        trading_derivative_indicator = self.trading_derivative_indicator
        share_number_sheet = self.share_number_sheet

        gmData_history['rpt_date'] = gmData_history['eob'].dt.strftime('%Y-%m-%d')
        gmData_history["rpt_date"] = pd.to_datetime(gmData_history['rpt_date'])
        gmData_history.drop(['bob', 'position', 'eob'], axis=1, inplace=True)

        # SH300 = history(
        #     symbol='SHSE.000300',
        #     frequency='1d',
        #     start_time=self.beginDate.strftime('%Y-%m-%d'),
        #     end_time=self.endDate.strftime('%Y-%m-%d'),
        #     fields=' close,eob,pre_close',
        #     df=True)
        # SH300['rpt_date'] = SH300['eob'].dt.strftime('%Y-%m-%d')
        # SH300["rpt_date"] = pd.to_datetime(SH300['rpt_date'])
        # SH300 = SH300.drop(['eob'], axis=1)
        # SH300 = SH300.reset_index(drop=True)
        # SH300 = SH300.rename(columns={'close': 'index_close', 'pre_close': 'pre_index_close'})

        SH300 = get_data(tableName='gmData_bench_price',
                         begin=self.beginDate,
                         end=self.endDate,
                         ticker=['000300'],
                         fields=['pre_close', 'trade_date'])
        SH300['close'] = SH300['pre_close'].shift(-1)
        SH300 = SH300.rename(columns={'close': 'index_close', 'pre_close': 'pre_index_close', 'trade_date': 'rpt_date'})
        SH300['rpt_date'] = SH300['rpt_date'].dt.strftime('%Y-%m-%d')
        SH300["rpt_date"] = pd.to_datetime(SH300['rpt_date'])
        SH300 = SH300.reset_index(drop=True)

        market_sheet = pd.merge(gmData_history, SH300, on=['rpt_date'], how='left')
        market_sheet = pd.merge(market_sheet, share_number_sheet, on=['symbol', 'rpt_date'], how='left')
        trading_derivative_indicator = trading_derivative_indicator.rename(columns={'end_date': 'rpt_date'})
        trading_derivative_indicator['rpt_date'] = trading_derivative_indicator['rpt_date'].dt.strftime('%Y-%m-%d')
        trading_derivative_indicator["rpt_date"] = pd.to_datetime(trading_derivative_indicator['rpt_date'])
        market_sheet = pd.merge(market_sheet, trading_derivative_indicator, on=['symbol', 'rpt_date'], how='left')
        market_sheet['pub_date'] = market_sheet['rpt_date']
        self.trading_derivative_indicator_modify = trading_derivative_indicator
        return market_sheet

    @cached_property
    def market_financial_sheet(self):
        # financial_sheet = self.financial_sheet
        # market_sheet = self.market_sheet

        financial_sheet = get_data('financial_sheet',
                                   begin=self.beginDate,
                                   end=self.endDate)

        market_sheet = get_data('market_sheet',
                                begin=self.beginDate,
                                end=self.endDate)

        fields = useful_field(self.joint_quant_factor, 'income_sheet')[3:] + \
                 useful_field(self.joint_quant_factor, 'cashflow_sheet')[3:]
        financial_sheet = change_frequency(financial_sheet, fields)
        market_financial_sheet = pd.merge(market_sheet, financial_sheet, on=['symbol', 'pub_date'], how='left')
        market_financial_sheet['rpt_date'] = market_sheet['pub_date']
        market_financial_sheet['share_total'] = market_financial_sheet['share_total_x']
        market_financial_sheet.drop(['share_total_x', 'share_total_y'], axis=1, inplace=True)
        market_financial_sheet['share_circ'] = market_financial_sheet['share_circ_x']
        market_financial_sheet.drop(['share_circ_x', 'share_circ_y'], axis=1, inplace=True)
        return market_financial_sheet

    @time_decorator
    def save(self, tableName,
             date_col,
             root_path=dataBase_root_path_JointQuant_prepare,
             freq='Y',
             **kwargs):
        if freq == 'Y':
            save_data_Y(getattr(self, tableName),
                        tableName=tableName,
                        date_column_name=date_col,
                        _dataBase_root_path=root_path,
                        reWrite=True)
        elif freq == 'Q':
            save_data_Q(getattr(self, tableName),
                        tableName=tableName,
                        date_column_name=date_col,
                        _dataBase_root_path=root_path,
                        reWrite=True)
        else:
            raise NotImplementedError
