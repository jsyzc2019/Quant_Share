from meta_jointqaunt_factorCalculator import GetOringinalData, get_joint_quant_factor
from meta_jointqaunt_factorCalculator.factors import *
from meta_jointqaunt_factorCalculator import MyMultiprocess, push_job, log_result
from multiprocessing import cpu_count, Manager
from Euclid_work.Quant_Share import printJson
from collections import defaultdict

joint_quant_factor = get_joint_quant_factor()
factor_list = joint_quant_factor['factor_name']
datas = GetOringinalData('20220101')
workers = MyMultiprocess(process_num=cpu_count())

# manager = Manager()
# return_dict = manager.dict()

result_dict = defaultdict(list)
workers.work(push_job, _vars=factor_list, constant=[joint_quant_factor, datas], callback=log_result)
printJson(result_dict)

