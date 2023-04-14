# -*- coding: utf-8 -*-
# @Time    : 2023/4/9 16:20
# @Author  : Euclid-Jie
# @File    : BK_test.py

from Utils import *

# score prepare
data = get_data('HKshszHold', begin='20200101', end='20221231')
data = data.pivot(index='endDate', columns='ticker', values='partyPct')
data = reindex(data.pct_change(periods=5, axis=0))
score = info_lag(data2score(data), n_lag=1)  # do not use future info

# group beck data prepare
DataClass = DataPrepare(beginDate='20200101', endDate='20221231')
DataClass.get_Tushare_data()

# group beck test
BTClass = simpleBT(DataClass.TICKER, DataClass.BENCH)
fig, outMetrics, group_res = BTClass.groupBT(score)
fig.show()
print(outMetrics)
