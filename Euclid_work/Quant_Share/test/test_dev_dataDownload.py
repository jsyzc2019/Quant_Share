import unittest
import pandas as pd
from Euclid_work.Quant_Share.dev_dataDownload.meta_gm_dataDownLoad import gmData_bench_price


class MyTestCase(unittest.TestCase):
    def test_meta_gm_dataDownLoad(self):
        data = gmData_bench_price(begin='20230620', end='20230621', symbol=['SZSE.000001'])

        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 2)


if __name__ == '__main__':
    unittest.main()
