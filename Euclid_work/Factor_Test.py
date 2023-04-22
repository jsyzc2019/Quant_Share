# -*- coding: utf-8 -*-
# @Time    : 2023/4/16 16:08
# @Author  : Euclid-Jie
# @File    : Factor_Test.py
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from Utils import get_data, reindex, FactorTest, info_lag, data2score

# score prepare
data = get_data('HKshszHold', begin='20200101', end='20221231')
data = data.pivot(index='endDate', columns='ticker', values='partyPct')
data = reindex(data.pct_change(periods=5, axis=0))
score = info_lag(data2score(data), n_lag=1)  # do not use future info

# Calc ICIR
FTClass = FactorTest(beginDate='20200101', endDate='20221231')
rankIC, IR = FTClass.calc_IC(score)
fig, axis = plt.subplots()
axis.bar(rankIC.index, rankIC['rankIC'], width=1, edgecolor="blue", linewidth=0.8)
axis.set_title("IR : {:.4f}".format(IR.values[0]))
axis.xaxis.set_major_locator(ticker.MultipleLocator(int(len(rankIC) / 5)))
fig.show()
