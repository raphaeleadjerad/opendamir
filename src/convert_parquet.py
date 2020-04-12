# Conversion to parquet files

# modules ---------------------------------------------
import dask.dataframe as dd
import os
import re
import time
import glob
print(os.getcwd())


# parameters -------------------------------------------
cols = ["PRS_REM_TYP", "PRS_PAI_MNT", "PRS_REM_MNT", "ETE_IND_TAA", "ASU_NAT",
        "PRS_REM_BSE", "PRS_NAT", "PSE_ACT_CAT"]

# import & conversion ------------------------------------


def convert_to_parquet(path2files, cols):
    """
    Function to convert files to parquet format, after selecting columns of interest
    :param path2files: List of all monthly files
    :param cols: List of strings, Columns to be selected in final output
    :return: Nothing, export to parquet in same directory
    """
    r = re.compile(r"A2019[0-9]{2}")
    for f in path2files:
        print(f)
        df = dd.read_csv(f, usecols=cols, sep=";", compression='gzip', blocksize=None)
        df = df.repartition(npartitions=20)
        # Filter : legal part only
        df = df.loc[df["PRS_REM_TYP"].isin([0, 1]), :]
        dd.to_parquet(df, "data/" + r.findall(f)[0])


# Application --------------------------------------------------
path2files = glob.glob('data/*.csv.gz')
start_time = time.time()
convert_to_parquet(path2files, cols)
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
print("Total elapsed time : {:.1f} m and {:.1f} seconds".format(m, s))  # 31 min