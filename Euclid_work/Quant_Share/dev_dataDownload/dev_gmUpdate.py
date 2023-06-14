from Euclid_work.Quant_Share import get_tradeDate, get_table_info
from meta_gm_dataDownLoad import *
from datetime import datetime

# from gm.api import *  # 接口文档 https://www.myquant.cn/docs/python/python_select_api
# 根据数据表最后写入的时间做增量更新
tableNameList = [
    # 'share_change', 'balance_sheet', 'deriv_finance_indicator', 'fundamentals_balance', 'fundamentals_income',
    # 'fundamentals_cashflow', 'trading_derivative_indicator', 'future_daily', 'continuous_contracts', 'symbol_industry',
    # 'gmData_history', 'gmData_bench_price'
]
for tableName in tableNameList:
    begin = get_tradeDate(get_table_info(tableName)['Modify Time'], -1)
    if begin < get_tradeDate(datetime.now(), -5):
        print("{} update from {} start!".format(tableName, begin.strftime('%Y%m%d')))
        eval(tableName + '_update(upDateBegin=begin)')
# AutoEmail('GM数据, 自动更新完成!')
