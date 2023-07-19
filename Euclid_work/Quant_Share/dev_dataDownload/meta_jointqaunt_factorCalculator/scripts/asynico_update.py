import json
import asyncio
import os
import traceback
from datetime import datetime

from tqdm import tqdm
from ..factors import *
from ..prepare import DataPrepare
from ..utils import choose_data, factor_max_rpt
from Euclid_work.Quant_Share import (
    dataBase_root_path_JointQuant_Factor,
    get_data,
    save_data_Y,
)
import time


async def update(func, factor_name, joint_quant_factor, data_prepare):
    try:
        file_path = os.path.join(dataBase_root_path_JointQuant_Factor, factor_name)
        data_name = choose_data(factor_name, joint_quant_factor)
        df = get_data(data_name, begin=data_prepare.beginDate, end=data_prepare.endDate)
        if os.path.exists(file_path):
            max_rpt = factor_max_rpt(file_path)

            max_rpt1 = df["rpt_date"].max()
            if max_rpt >= max_rpt1:
                return "根据quant_share中的数据，" + factor_name + "因子不需要更新\n"
            else:
                if "close" in df.columns:
                    start_date = str(max_rpt.date().year - 3) + "-01-01"
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
                    df = df[df["rpt_date"] >= start_date]
        factor = func(df)
        save_data_Y(
            df=factor,
            date_column_name="rpt_date",
            tableName=factor_name,
            _dataBase_root_path=dataBase_root_path_JointQuant_Factor,
            reWrite=True,
        )
        return factor_name + "更新完毕\n"
    except:
        return factor_name + traceback.format_exc()


async def main():
    start = time.perf_counter()
    dp = DataPrepare("20200101")

    joint_quant_factor = dp.joint_quant_factor
    factor_list = joint_quant_factor["factor_name"]

    tasks = []
    for fn in tqdm(factor_list):
        task = asyncio.create_task(
            update(
                func=globals()[fn],
                factor_name=fn,
                joint_quant_factor=joint_quant_factor,
                data_prepare=dp,
            )
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    with open("results.txt", "w") as fp:
        fp.writelines(results)

    end = time.perf_counter()
    print(f"程序运行时间:{end - start}秒")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
