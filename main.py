from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from korean_romanizer.romanizer import Romanizer

import data
import x13

plt.style.use("./auri.mplstyle")

pd.options.display.unicode.east_asian_width = True


def run_x13(s: pd.Series, name=None, x12path="./x13as/x13as.exe"):
    if name is None:
        name = s.name

    # romanize name for American x13
    name_romanized = Romanizer(name).romanize()
    s_new = s.copy().rename(name_romanized)

    results = x13.x13_arima_analysis(s_new, x12path=x12path)
    # return results

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

        if not Path(f"output/{name}.csv").exists():
            df_result, fig, order, sorder = run_x13(s, name)

            df_result.to_csv(f"output/{name}.csv", encoding="utf-8-sig")
            fig.savefig(f"output/{name}.png")
            fig.savefig(f"output/{name}.svg")
        break
    break
