
import os
os.chdir('E:\yuankangrui\Quant_Share_Local')
import sys
sys.path.append('E:\yuankangrui\Quant_Share_Local')
sys.path.append('E:\yuankangrui\Quant_Share_Local\Euclid_work\Quant_Share\dev_dataDownload')
from meta_jointqaunt_factorCalculator.prepare import DataPrepare
if __name__ == "__main__":
    # dp = DataPrepare('20180101')
    # dp.save('financial_sheet', date_col='rpt_date')
    # dp.save('market_sheet', date_col='rpt_date')
    # dp.save('ResConSecCorederi_sheet', date_col='rpt_date')

    dp = DataPrepare("20180101")
    dp.save("market_financial_sheet", date_col="rpt_date")
