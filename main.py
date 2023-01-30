from pathlib import Path
import re
import traceback
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from korean_romanizer.romanizer import Romanizer
from statsmodels.tsa.x13 import X13Error

import data
import x13


plt.style.use("./auri.mplstyle")

pd.options.display.unicode.east_asian_width = True


def run_x13(s: pd.Series, name=None, x12path="./x13as/x13as.exe"):
    if name is None:
        name = s.name

    # copy original series
    s_new = s.copy()

    # romanize name for American x13
    name_romanized = Romanizer(name).romanize()
    s_new = s_new.rename(name_romanized)

    # trim missing values at each ends
    s_new = s_new.truncate(s_new.first_valid_index(), s_new.last_valid_index())

    # fill missing values in the middle
    s_new = s_new.fillna(0)

    try:
        results = x13.x13_arima_analysis(s_new, x12path=x12path)
    except X13Error as e:
        raise e

    arima_results = x13.get_arima_order_from_results(results)
    order, sorder = arima_results.order, arima_results.sorder

    df_result = pd.concat(
        [
            results.observed,
            results.seasadj,
            results.trend,
            results.seasonal,
            results.irregular,
        ],
        axis="columns",
    )

    fig = results.plot()
    return df_result, fig, order, sorder


Path("output").mkdir(exist_ok=True)

dfs_approval = data.process_data()

for key, df in dfs_approval.items():
    for label, s in df.items():
        name = "_".join([key, *s.name])
        # sanitize name
        name = re.sub("\\W", "_", name)

        print(f"{name}...", end=" ")

        if not Path(f"output/{name}.csv").exists():
            try:
                df_result, fig, order, sorder = run_x13(s, name)

                df_result.to_csv(f"output/{name}.csv", encoding="utf-8-sig")
                fig.savefig(f"output/{name}.png")
                fig.savefig(f"output/{name}.svg")

            except X13Error as e:
                print("error")
                Path("error").mkdir(exist_ok=True)
                with open(f"error/{name}.txt", "w") as f:
                    traceback.print_exc(file=f)
            else:
                print("done")
        else:
            print("already exists")
