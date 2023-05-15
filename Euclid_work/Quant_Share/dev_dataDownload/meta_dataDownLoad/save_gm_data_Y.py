"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/15 9:49
# @Author  : Euclid-Jie
# @File    : save_gm_data_Y.py
"""
from .base_package import *


def save_gm_data_Y(df, date_column_name, tableName, dataBase_root_path=dataBase_root_path_gmStockFactor, reWrite=False):
    print("数据将存储在: {}/{}".format(dataBase_root_path, tableName))
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        df1 = df1.drop(['year'], axis=1)
        save_data_h5(df1, name='{}_Y{}'.format(tableName, yeari),
                     subPath="{}/{}".format(dataBase_root_path, tableName), reWrite=reWrite)
