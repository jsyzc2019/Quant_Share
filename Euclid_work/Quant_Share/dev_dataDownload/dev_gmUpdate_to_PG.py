"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/18 22:21
# @Author  : Euclid-Jie
# @File    : dev_gmUpdate_to_PG.py
# @Desc    : 用于更新数据至postgres仓库
"""
import subprocess
import os

meta_path = r"E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_dataDownload\meta_gm_data_update_to_PG"
for file in os.listdir(meta_path):
    if file.endswith("update_to_PG.py"):
        print("{} update begin...".format(file.split("_update_to_PG.py")[0]))
        subprocess.run(
            [
                "python",
                os.path.join(meta_path, file),
            ],
        )
