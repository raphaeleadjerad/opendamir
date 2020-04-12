# REDUCTION OPEN DAMIR

import dask.dataframe as dd
import pandas as pd


def opendamir_reduce_by_month(path2file):
    """
    Function to reduce one month of opendamir into a database indexed by PRS_NAT_REF
    and with expenditure indicators as variables
    :param path2file: String, Monthly compressed file
    :return: pandas DataFrame, Monthly expenditure database with summed columns by act
    """
    df = dd.read_parquet(path2file)
    # Filters : this can be adapted to specific problem
    df = df.loc[df["ETE_IND_TAA"] != 1, :]
    df = df.groupby(["PRS_NAT", "PSE_ACT_CAT"]).agg({"PRS_PAI_MNT": "sum", "PRS_REM_MNT": "sum"}).compute()
    return df


def opendamir_reduce(path2files):
    """
    Function to concatenate all OPENDAMIR in aggregated form
    :param path2files: List of strings, Path containing all OPENDAMIR parquet paths
    :return: pandas DataFrame, Annual data set
    """
    appended_data = []
    for f in path2files:
        print(f)
        dataset = opendamir_reduce_by_month(f)
        appended_data.append(dataset)
    appended_data = pd.concat(appended_data, axis=1, join="outer")
    appended_data = appended_data.fillna(0)
    appended_data = appended_data.groupby(appended_data.columns, axis=1).sum()
    return appended_data


def compute_opendamir(df, categ_nom):
    """
    Function that takes the reduced OPENDAMIR as input to produce total cost by
    categories defined in the `categ_nom` parameter
    :param df: pandas DataFrame
    :param categ_nom: pandas DataFrame, input for nomenclature
    :return: pandas DataFrame, resulting output for categories defined in categ_nom
    """
    df = df.reset_index()
    df = pd.merge(df, categ_nom, on="PRS_NAT", how="outer")
    df = df.rename(columns={categ_nom.columns[1]: "categ"})
    df.loc[df["categ"] != "cliniq", "PSE_ACT_CAT"] = ""
    df = df.groupby(["categ", "PSE_ACT_CAT"]).agg({"PRS_PAI_MNT": "sum", "PRS_REM_MNT": "sum"})
    df = df.sort_values("PRS_PAI_MNT", ascending=False)
    return df
