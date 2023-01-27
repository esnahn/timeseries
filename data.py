import os
from os import PathLike
from typing import Callable, Mapping, Sequence, Union
from pathlib import Path
import pandas as pd
from datetime import datetime

__all__ = [
    "dfs_approval",
    "df_approval_by_use6",
    "df_approval_by_use28",
    "df_approval_by_activity",
    "df_approval_by_structure",
    "df_approval_by_sido",
]

pd.options.display.unicode.east_asian_width = True


### use6

paths_use6 = [
    Path("data/동수별_연면적별_건축허가현황_2001.csv"),
    Path("data/동수별_연면적별_건축허가현황_2022.csv"),
]

dtype_use6 = {
    ("연면적별", "합계", "합계"): "float64",
    ("연면적별", "용도별", "주거용"): "float64",
    ("연면적별", "용도별", "상업용"): "float64",
    ("연면적별", "용도별", "공업용"): "float64",
    ("연면적별", "용도별", "교육및사회용"): "float64",
    ("연면적별", "용도별", "기타"): "float64",
}

df_approval_by_use6 = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[0, 1, 2],
            index_col=[0, 1],
            parse_dates=[1],
            dtype=dtype_use6,
        )
        for path in paths_use6
    ]
)

# drop repeated indexes
df_approval_by_use6.index = df_approval_by_use6.index.droplevel(0)  # 총합계
df_approval_by_use6.columns = df_approval_by_use6.columns.droplevel(1)  # 용도별

# match column names to 시도별 건축허가현황
if df_approval_by_use6.columns.levels[0].to_list() == ["동수별", "연면적별"]:
    df_approval_by_use6.columns = df_approval_by_use6.columns.set_levels(
        ["동수", "연면적"], level=0
    )
else:
    raise ValueError("column names are different from the expected values")

# give meaningful names to column levels
df_approval_by_use6.columns.names = ["value", "use6"]

# print(df_approval_by_use6)


### use28

paths_use28_count = [
    Path("data/시도별_건축허가현황_용도_동수_2011.csv"),
    Path("data/시도별_건축허가현황_용도_동수_2014.csv"),
]

paths_use28_floorarea = [
    Path("data/시도별_건축허가현황_용도_연면적_2011.csv"),
    Path("data/시도별_건축허가현황_용도_연면적_2014.csv"),
]


df_approval_by_use28_count = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[0],
            index_col=[1, 2, 5],
            parse_dates=[5],
            date_parser=lambda x: datetime.strptime(x, "%Y.%m 월"),
        )
        for path in paths_use28_count
    ]
)
df_approval_by_use28_floorarea = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[0],
            index_col=[1, 2, 5],
            parse_dates=[5],
            date_parser=lambda x: datetime.strptime(x, "%Y.%m 월"),
        )
        for path in paths_use28_floorarea
    ]
)

# extract the value series
s_approval_by_use28_count = df_approval_by_use28_count.계.rename("동수")
s_approval_by_use28_floorarea = df_approval_by_use28_floorarea.계.rename("연면적")

# remove original dataframe
del df_approval_by_use28_count
del df_approval_by_use28_floorarea

# print(s_approval_by_use28_count.groupby(level=[0, 1]).median())
# print(s_approval_by_use28_floorarea.groupby(level=[0, 1]).median())

# combine series into a dataframe
df_approval_by_use28 = pd.concat(
    [
        s_approval_by_use28_count,
        s_approval_by_use28_floorarea,
    ],
    axis="columns",
).unstack(level=[0, 1])

# give meaningful names to column levels
df_approval_by_use28.columns.names = ["value", "use28", "use28_sub"]


# remove original series
del s_approval_by_use28_count
del s_approval_by_use28_floorarea


### activity

paths_activity = [
    Path("data/시도별_건축허가현황_건축행위.csv"),
]
df_approval_by_activity = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[0, 1],
            index_col=[4],
            parse_dates=[4],
        )
        for path in paths_activity
    ]
).iloc[:, 4:]

# give meaningful names to column levels
df_approval_by_activity.columns.names = ["value", "activity"]

### structure

paths_structure = [
    Path("data/시도별_건축허가현황_구조.csv"),
]
df_approval_by_structure = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[3, 5],
            index_col=[0],
            parse_dates=[0],
        )
        for path in paths_structure
    ]
).iloc[:, :]

# give meaningful names to column levels
df_approval_by_structure.columns.names = ["value", "structure"]

# print(df_approval_by_structure)


### sido

paths_sido = [
    Path("data/시도별_건축허가현황_시도.csv"),
]
df_approval_by_sido = pd.concat(
    [
        pd.read_csv(
            Path(path),
            encoding="cp949",
            header=[0, 1],
            skiprows=[2, 3, 4, 5],
            index_col=[0],
            parse_dates=[0],
        )
        for path in paths_sido
    ]
).iloc[:, :]

# give meaningful names to column levels
df_approval_by_sido.columns.names = ["value", "sido"]

# print(df_approval_by_sido)


dfs_approval = {
    "use6": df_approval_by_use6,
    "use28": df_approval_by_use28,
    "activity": df_approval_by_activity,
    "structure": df_approval_by_structure,
    "sido": df_approval_by_sido,
}
