"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/18 22:21
# @Author  : Euclid-Jie
# @File    : dev_gmUpdate_to_PG.py
# @Desc    : 用于更新数据至postgres仓库
"""
from meta_gm_dataDownLoad import *
from Euclid_work.Quant_Share.warehouse import *

gmData_history_1m_update(
    upDateBegin="20230717", endDate="20230718", engine=postgres_engine()
)
