import unittest
import pandas as pd
from Euclid_work.Quant_Share import stockNumList, patList, tradeDateList, format_date
from Euclid_work.Quant_Share.dev_dataDownload.meta_gm_dataDownLoad import gmData_bench_price
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad.FakeDataAPI import FakeDataAPI as DataAPI


class MyTestCase(unittest.TestCase):
    def test_meta_gm_dataDownLoad(self):
        data = gmData_bench_price(begin='20230620', end='20230621', symbol=['SZSE.000001'])
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 2)

    def test_FakeDataAPI_MktEqudGet(self):
        data = DataAPI.MktEqudGet(ticker='000001', beginDate=20210101, endDate=20210110)
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 5)

    def test_FakeDataAPI_MktLimitGet(self):
        data = DataAPI.MktLimitGet(ticker='000001', beginDate=20210101, endDate=20210110)
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) == 5)

    def test_FakeDataAPI_FdmtIndiRtnPitGet(self):
        data = DataAPI.FdmtIndiPSPitGet(ticker='000001', beginDate=20200101, endDate=20220110, reportType=["A"])
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) > 0)

    def test_FakeDataAPI_FdmtIndiPSPitGet(self):
        data = DataAPI.FdmtIndiPSPitGet(ticker='000001', beginDate=20200101, endDate=20220110, reportType=["A"])
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) > 0)

    def test_FakeDataAPI_mIdxCloseWeightGet(self):
        data = DataAPI.mIdxCloseWeightGet(ticker='000300', beginDate='20220101', endDate='20230110')
        self.assertEqual(True, isinstance(data, pd.DataFrame) and len(data) > 0)


if __name__ == '__main__':
    unittest.main()
