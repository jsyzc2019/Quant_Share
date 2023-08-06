"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/18 22:21
# @Author  : Euclid-Jie
# @File    : dev_gmUpdate_to_PG.py
# @Desc    : 用于更新数据至postgres仓库
"""
import subprocess
import os
from Euclid_work.Quant_Share.dev_dataDownload.meta_gm_dataDownLoad.AutoEmail import (
    AutoEmail,
)

meta_path = r"E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_dataDownload\meta_gm_data_update_to_PG"
for file in os.listdir(meta_path):
    if file.endswith("update_to_PG.py"):
        print("{} update begin...".format(file.split("_update_to_PG.py")[0]))
        subprocess.run(
            [
                r"E:\Euclid\Quant_Share\venv\Scripts\python.exe",
                os.path.join(meta_path, file),
            ],
        )

AutoEmail(
    title="gm data update finished",
    content=None,
    sender="2379928684@qq.com",
    config_path=r"E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_dataDownload\meta_gm_dataDownLoad\AutoEmail_config.txt",
)
