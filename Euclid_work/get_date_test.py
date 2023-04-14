# -*- coding: utf-8 -*-
# @Time    : 2023/4/9 11:28
# @Author  : Euclid-Jie
# @File    : get_date_test.py
from Utils import get_data

data = get_data('FdmtDerPit', begin='20180101', end=None, ticker='000001')
print(data.info())
