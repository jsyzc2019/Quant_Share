import json
from meta_jointqaunt_factorCalculator.factors import *
from meta_jointqaunt_factorCalculator.prepare import DataPrepare
from meta_jointqaunt_factorCalculator.utils import update
from multiprocessing import cpu_count, Manager, Pool
from Euclid_work.Quant_Share import printJson
from collections import defaultdict
import time

if __name__ == '__main__':

    start = time.perf_counter()
    dp = DataPrepare('20180101')
    dp.save('financial_sheet', date_col='rpt_date')
    dp.save('market_sheet', date_col='rpt_date')
    dp.save('ResConSecCorederi_sheet', date_col='rpt_date')
    dp.save('market_financial_sheet', date_col='rpt_date')

    # result_dict = dict()
    # def log_result(result):
    #     result_dict.update(result)
    # pool = Pool(processes=cpu_count() // 4)
    # joint_quant_factor = dp.joint_quant_factor
    # factor_list = joint_quant_factor['factor_name']
    #
    # for factor_name in factor_list:
    #     pool.apply_async(update,
    #                      kwds={
    #                         'func':globals()[factor_name],
    #                         'factor_name': factor_name,
    #                         'joint_quant_factor': joint_quant_factor,
    #                         'data_prepare': dp
    #                            },
    #                      callback=log_result)
    # pool.close()
    # pool.join()
    #
    # printJson(result_dict)
    # with open('error.json', 'w') as fp:
    #     json.dump(result_dict, fp, indent=4)
    #
    # end = time.perf_counter()
    # print(f'程序运行时间:{end - start}秒')