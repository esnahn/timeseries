import os
from os import PathLike
from typing import Callable, Mapping, Sequence, Union
from pathlib import Path
import pandas as pd
from datetime import datetime

__all__ = ["process_data"]

pd.options.display.unicode.east_asian_width = True


def process_data():
    return {
        "use6": process_data_use6(),
        "use28": process_data_use28(),
        "activity": process_data_activity(),
        "structure": process_data_structure(),
        "sido": process_data_sido(),
    }


def process_data_use6():
    paths = [
        Path("data/동수별_연면적별_건축허가현황_2001.csv"),
        Path("data/동수별_연면적별_건축허가현황_2022.csv"),
    ]

    dtypes = {
        ("연면적별", "합계", "합계"): "float64",
        ("연면적별", "용도별", "주거용"): "float64",
        ("연면적별", "용도별", "상업용"): "float64",
        ("연면적별", "용도별", "공업용"): "float64",
        ("연면적별", "용도별", "교육및사회용"): "float64",
        ("연면적별", "용도별", "기타"): "float64",
    }

    df = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[0, 1, 2],
                index_col=[0, 1],
                parse_dates=[1],
                dtype=dtypes,
            )
            for path in paths
        ]
    )

    # drop repeated indexes
    df.index = df.index.droplevel(0)  # 총합계
    df.columns = df.columns.droplevel(1)  # 용도별

    # match column names to 시도별 건축허가현황
    if df.columns.levels[0].to_list() == ["동수별", "연면적별"]:
        df.columns = df.columns.set_levels(["동수", "연면적"], level=0)
    else:
        raise ValueError("column names are different from the expected values")

    # give meaningful names to column levels
    df.columns.names = ["value", "use6"]

    return df


def process_data_use28():
    paths_count = [
        Path("data/시도별_건축허가현황_용도_동수_2011.csv"),
        Path("data/시도별_건축허가현황_용도_동수_2014.csv"),
    ]

    paths_floorarea = [
        Path("data/시도별_건축허가현황_용도_연면적_2011.csv"),
        Path("data/시도별_건축허가현황_용도_연면적_2014.csv"),
    ]

    df_count = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[0],
                index_col=[1, 2, 5],
                parse_dates=[5],
                date_parser=lambda x: datetime.strptime(x, "%Y.%m 월"),
            )
            for path in paths_count
        ]
    )
    df_floorarea = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[0],
                index_col=[1, 2, 5],
                parse_dates=[5],
                date_parser=lambda x: datetime.strptime(x, "%Y.%m 월"),
            )
            for path in paths_floorarea
        ]
    )

    # extract the value series
    s_count = df_count.계.rename("동수")
    s_floorarea = df_floorarea.계.rename("연면적")

    # # remove original dataframe
    # del df_approval_by_use28_count
    # del df_approval_by_use28_floorarea

    # print(s_approval_by_use28_count.groupby(level=[0, 1]).median())
    # print(s_approval_by_use28_floorarea.groupby(level=[0, 1]).median())

    # combine series into a dataframe
    df = pd.concat(
        [
            s_count,
            s_floorarea,
        ],
        axis="columns",
    ).unstack(level=[0, 1])

    # give meaningful names to column levels
    df.columns.names = ["value", "use28", "use28_sub"]

    # combine columns with name change and drop old column
    for c0 in df.columns.levels[0]:
        for col_old, col_new in [
            (
                ("분뇨.쓰레기처리시설", "분뇨.쓰레기처리시설"),
                ("자원순환관련시설", "자원순환관련시설"),
            ),
        ]:
            df[(c0, *col_new)] = df[(c0, *col_new)].combine_first(df[(c0, *col_old)])
            df = df.drop((c0, *col_old), axis="columns")

    # # remove original series
    # del s_approval_by_use28_count
    # del s_approval_by_use28_floorarea

    return df


def process_data_activity():
    paths = [
        Path("data/시도별_건축허가현황_건축행위.csv"),
    ]
    df = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[0, 1],
                index_col=[4],
                parse_dates=[4],
            )
            for path in paths
        ]
    ).iloc[:, 4:]

    # give meaningful names to column levels
    df.columns.names = ["value", "activity"]

    return df


def process_data_structure():
    paths = [
        Path("data/시도별_건축허가현황_구조.csv"),
    ]
    df = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[3, 5],
                index_col=[0],
                parse_dates=[0],
            )
            for path in paths
        ]
    ).iloc[:, :]

    # give meaningful names to column levels
    df.columns.names = ["value", "structure"]

    return df


def process_data_sido():
    paths = [
        Path("data/시도별_건축허가현황_시도.csv"),
    ]
    df = pd.concat(
        [
            pd.read_csv(
                Path(path),
                encoding="cp949",
                header=[0, 1],
                skiprows=[2, 3, 4, 5],
                index_col=[0],
                parse_dates=[0],
            )
            for path in paths
        ]
    ).iloc[:, :]

    # give meaningful names to column levels
    df.columns.names = ["value", "sido"]

    return df
