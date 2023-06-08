from io import StringIO
from typing import Union
import auri
import pandas as pd
from pathlib import Path
from pprint import pprint

results = Path("data/x13")

d11 = results.glob("*.d11")  # final seasonally adjusted data
d12 = results.glob("*.d12")  # final trend cycle
saa = results.glob("*.saa")

# pprint(list(d11))
# pprint(list(saa))


def read_x13_output(path: Union[str, Path]) -> pd.Series:
    path = Path(path)
    with open(str(path), "r") as f:
        df = (
            pd.read_csv(
                StringIO(f.read()),
                skiprows=2,
                header=None,
                index_col=0,
                sep="\t",
                engine="python",
            )
            .rename_axis(index={0: "date"})
            .rename(columns={0: "date", 1: path.stem})
        )
    return df


col_order = pd.read_csv("col_order.txt", header=None)[0].tolist()
print(col_order)


def reorder_cols(df: pd.DataFrame, col_order) -> pd.DataFrame:
    real_order = [col for col in col_order if col in df.columns]
    return df[real_order].copy()


def do_results(paths, col_order):
    df = pd.concat([read_x13_output(f) for f in paths], axis="columns")
    df = reorder_cols(df, col_order)
    return df


df_d11 = do_results(d11, col_order)
df_d11.to_csv(f"output/x13results_d11.csv", encoding="utf-8-sig")

df_d12 = do_results(d12, col_order)
df_d12.to_csv(f"output/x13results_d12.csv", encoding="utf-8-sig")

df_saa = do_results(saa, col_order)
df_saa.to_csv(f"output/x13results_saa.csv", encoding="utf-8-sig")
