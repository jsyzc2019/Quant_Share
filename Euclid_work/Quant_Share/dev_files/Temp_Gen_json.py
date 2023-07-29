import json
import pandas as pd
from pathlib import Path

current_dir = Path(__file__).parent

joint_quant = pd.read_excel(current_dir / "jointquant_factor.xlsx")

joint_quant_names = joint_quant["factor_name"]

res = {}
for i in joint_quant_names:
    res[str(i).strip()] = {
        "assets": "jointquant_factor",
        "description": "",
        "date_column": "rpt_date",
        "ticker_column": "symbol"
    }
with open(current_dir / "jointquant_factor.json", "w") as fp:
    json.dump(res, fp, indent=4)
