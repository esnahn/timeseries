import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from korean_romanizer.romanizer import Romanizer

import data
import x13

plt.style.use("./auri.mplstyle")

pd.options.display.unicode.east_asian_width = True

# columns = []
# for key, df in data.dfs_approval.items():
#     df.apply(lambda x: columns.append("_".join([key, *x.name])))
# print(columns)


def get_x13_order(s, x12path="./x13as/x13as.exe"):
    res = x13.x13_arima_select_order(s, x12path=x12path)
    return res.order, res.sorder


for key, df in data.dfs_approval.items():
    for label, s in df.items():
        print(key)

        # romanize name for American x13
        s_name_orig = "_".join([key, *s.name])
        s_name_latin = Romanizer(s_name_orig).romanize()
        s_new = s.copy().rename(s_name_latin)

        print(s_new.name)
        # print(get_x13_order(s_new))

        results = x13.x13_arima_analysis(s_new, x12path="./x13as/x13as.exe")
        fig = results.plot()
        fig.tight_layout()
        fig.show()
        input("press enter to exit...")
        break
    break
