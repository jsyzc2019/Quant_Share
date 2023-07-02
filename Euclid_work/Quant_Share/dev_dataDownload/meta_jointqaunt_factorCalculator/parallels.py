from multiprocessing import Pool
from .utils import update
from Euclid_work.Quant_Share import time_decorator

class MyMultiprocess(object):
    def __init__(self, process_num):
        self.pool = Pool(processes=process_num)

    @time_decorator
    def work(self, func, _vars, constant, callback):
        for arg in _vars:
            self.pool.apply_async(func, (arg, *constant, ), callback=callback)
        self.pool.close()
        self.pool.join()

def log_result(result):
    global result_dict
    for k, v in result.items():
        result_dict[k].append(v)


def push_job(factor_name, joint_quant_factor, datas):
    factor_name = factor_name.strip()
    func = globals()[factor_name]
    update(func, factor_name, joint_quant_factor, datas)
