from io import StringIO
from pathlib import Path
from typing import Union

import pandas as pd

# import auri
# from pprint import pprint

bokx13_path = Path("C:/BOKX13")
data_path = bokx13_path / "data"
# spec_path = bokx13_path / "spec"
out_path = bokx13_path / "out"

# CHANGE THIS!!! set input and output paths
dta_path = data_path / "20250617_reorder.dta"
save_to_path = Path("output")

results = {
    "d11": out_path.glob("*.d11"),  # final seasonally adjusted data
    # "d12": out_path.glob("*.d12"),  # final trend cycle
    "saa": out_path.glob(
        "*.saa"
    ),  # final seasonally adjusted series with forced yearly totals
}


def read_x13_output(path: Union[str, Path]) -> pd.DataFrame:
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
            .rename(columns={0: "date", 1: Path(path).stem})
        )
    return df


col_order = pd.read_csv(dta_path, header=None)[0].apply(lambda p: Path(p).stem).tolist()
print(col_order)


def reorder_cols(df: pd.DataFrame, col_order) -> pd.DataFrame:
    real_order = [col for col in col_order if col in df.columns]
    return df[real_order].copy()


def do_results(paths, col_order):
    df = pd.concat([read_x13_output(f) for f in paths], axis="columns")
    df = reorder_cols(df, col_order)
    return df


for key, files in results.items():
    print(f"Processing {key} files...")
    df = do_results(results[key], col_order)
    df.to_csv(save_to_path / f"x13results_{key}.csv", encoding="utf-8-sig")
