import json

from tqdm import tqdm

from ..factors import *
from ..prepare import DataPrepare
from ..utils import update
from multiprocessing import cpu_count, Manager, Pool
from Euclid_work.Quant_Share import printJson, patList
from collections import defaultdict
import time
import gc

if __name__ == '__main__':

    start = time.perf_counter()
    dp = DataPrepare('20200101')

    result_dict = dict()


    def log_result(result):
        result_dict.update(result)


    # pool = Pool(processes=cpu_count() // 4)
    joint_quant_factor = dp.joint_quant_factor
    factor_list = joint_quant_factor['factor_name']

    for fn in tqdm(factor_list):
        update(
            func=globals()[fn],
            factor_name=fn,
            joint_quant_factor=joint_quant_factor,
            data_prepare=dp,
            callback=log_result
        )

    # for factor_name in patList(factor_list, 30):
    #     pool = Pool(2)
    #     for fn in factor_name:
    #         job = pool.apply_async(update,
    #                                kwds={
    #                                    'func': globals()[fn],
    #                                    'factor_name': fn,
    #                                    'joint_quant_factor': joint_quant_factor,
    #                                    'data_prepare': dp
    #                                },
    #                                callback=log_result)
    #         if len(pool._cache) > 1e3:
    #             print("waiting for cache to clear...")
    #             job.wait()
    #     pool.close()
    #     pool.join()
    #     pool.terminate()

    printJson(result_dict)
    with open('./meta_jointqaunt_factorCalculator/error.json', 'w') as fp:
        json.dump(result_dict, fp, indent=4)

    end = time.perf_counter()
    print(f'程序运行时间:{end - start}秒')
