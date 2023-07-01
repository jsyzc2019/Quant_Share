"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/27 22:22
# @Author  : Euclid-Jie
# @File    : BK_test_signal.py
"""
from Euclid_work.Quant_Share.BackTest_signal import *
from Euclid_work.Quant_Share.Utils import printJson

DataClass = DataPrepare_signal("20180101", "20221231")
DataClass.get_Tushare_data()

# group beck test
BTClass = simpleBT_signal(DataClass.TICKER, DataClass.BENCH, negValueAdjust=False)
# 市值越小因子越大
Score = 1 - DataClass.Score
# 仅取top pct 10因子
Score[Score < Score.quantile(0.9)] = np.nan

fig, result, nav, pos_out, alpha_nav, daily_rtn = BTClass.main_back_test(
    Score, plot_begin=20200101, fee_rate=0.001, daily_rtn=True
)
fig.show()
printJson(result)
