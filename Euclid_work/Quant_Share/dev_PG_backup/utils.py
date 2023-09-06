"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/27 22:15
# @Author  : Euclid-Jie
# @File    : utils.py
# @Desc    : 更新指定h5文件的内容, 自动识别增量进行更新, 注意h5文件的format = "table":
            old(exited): [1, 2, 3, 4]
            add: [2, 3, 4, 5, 6]
            after update: [1, 2, 3, 4, 5, 6]
"""
import pandas as pd
from pathlib import Path
import os


def update_h5_file(data: pd.DataFrame, full_path: Path):
    """
    不考虑使用append参数, 每次直接读取已有数据做去重后更新
    """
    # format confirm
    assert full_path.suffix == ".h5", "full path should with '.h5' suffix"
    full_path.parent.mkdir(exist_ok=True)

    # new save
    if not full_path.exists():
        if len(data)>0:
            data.to_hdf(full_path, key="a", mode="w", format="table")
            print("{} has been created!".format(full_path))
    # update
    else:
        try:
            existsData = pd.read_hdf(full_path)
            assert (
                    existsData.columns == data.columns
            ).all(), "if update data, should have same columns"
            allData = pd.concat([existsData, data]).drop_duplicates()
            if len(allData) > len(existsData):
                allData.to_hdf(full_path, key="a", mode="w", format="table")
                print("{} has been updated!".format(full_path))
            else:
                print("{} has none Updated!".format(full_path))
        except ValueError:
            if len(data) > 0:
                data.to_hdf(full_path, key="a", mode="w", format="table")
                print("{} has been created!".format(full_path))


def get_all_file(tablePath: Path | str, query=".h5"):
    # 得到所有文件
    file_list = []
    file_full_path_list = []
    for path, file_dir, files in os.walk(tablePath):
        file_list.extend([file_name for file_name in files if file_name.endswith(query)])
        file_full_path_list.extend(
            [
                os.path.join(path, file_name)
                for file_name in files
                if file_name.endswith(query)
            ]
        )
    return file_list, file_full_path_list
