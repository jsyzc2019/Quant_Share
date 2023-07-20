import json
from tqdm import tqdm
from meta_jointqaunt_factorCalculator.factors import *
from meta_jointqaunt_factorCalculator.prepare import DataPrepare
from meta_jointqaunt_factorCalculator.utils import update
from multiprocessing import cpu_count
from Euclid_work.Quant_Share import printJson
import time
import concurrent.futures

result_dict = dict()


def log_result(result):
    result_dict.update(result)


def main():
    start = time.perf_counter()
    dp = DataPrepare("20200101")

    joint_quant_factor = dp.joint_quant_factor
    factor_list = joint_quant_factor["factor_name"]

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=cpu_count() // 4
    ) as executor:
        futures = []
        for fn in tqdm(factor_list):
            future = executor.submit(
                update,
                func=globals()[fn],
                factor_name=fn,
                joint_quant_factor=joint_quant_factor,
                data_prepare=dp,
                callback=log_result,
            )
            futures.append(future)

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

    printJson(result_dict)
    with open("./meta_jointqaunt_factorCalculator/error.json", "w") as fp:
        json.dump(result_dict, fp, indent=4)

    end = time.perf_counter()
    print(f"程序运行时间:{end - start}秒")


if __name__ == "__main__":
    main()
