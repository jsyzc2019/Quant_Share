import unittest
import pandas as pd
from Euclid_work.Quant_Share import stockNumList, patList, tradeDateList, format_date
from Euclid_work.Quant_Share.dev_dataDownload.meta_gm_dataDownLoad import gmData_bench_price
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad import FakeDataAPI as DataAPI


class MyTestCase(unittest.TestCase):
    def test_meta_gm_dataDownLoad(self):
        data = gmData_bench_price(begin='20230620', end='20230621', symbol=['SZSE.000001'])
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 2)

    def test_FakeDataAPI(self):
        data = DataAPI.MktEqudGet(ticker=stockNumList[0:10], tradeDate=u"", beginDate=20210101, endDate=20210110)
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 90)

        data = DataAPI.MktEqudGet(ticker='000001', tradeDate=u"", beginDate=20210101, endDate=20210110)
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 5)


if __name__ == '__main__':
    unittest.main()
