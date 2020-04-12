# TEST COMPILE_OPENDAMIR


import pandas as pd
import os
import time
import glob
import csv
import re
import seaborn as sns
import matplotlib.pyplot as plt
import src.opendamir as opendamir

print(os.getcwd())

# parameters
glob.glob('data/*.csv.gz')
r = re.compile(r"A2019[0-9]{2}")


# Application
path2files = glob.glob('data/*.csv.gz')
path2files = ["data/" + r.findall(f)[0] + "/" for f in path2files]
start_time = time.time()
dataset = opendamir.opendamir_reduce(path2files)
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
print("Total elapsed time : {:.1f} m and {:.1f} seconds".format(m, s))  # reduced to 1 min for whole base
dataset.memory_usage(index=True, deep=True).sum()
dataset.to_csv("data/opendamir_2019.csv", encoding="utf-8", quoting=csv.QUOTE_NONNUMERIC)


# Application compute_opendamir
categ_nom = pd.read_csv("data/categ_nom.csv", dtype=str)
categ_nom["PRS_NAT"] = categ_nom["PRS_NAT"].astype("int")

output = opendamir.compute_opendamir(dataset, categ_nom)

output = output.reset_index().assign(Expenditure=lambda df: df["PRS_PAI_MNT"]/1000000000)
output = output.assign(Categorie=lambda df: df["categ"] + " " + df["PSE_ACT_CAT"].astype(str))

# visualize result
plt.figure(figsize=(16, 10))
sns.barplot(y="Categorie", x="Expenditure", data=output.nlargest(columns="Expenditure", n=10), orient="h")


