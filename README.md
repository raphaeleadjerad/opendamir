# OPENDAMIR Analysis


This project aims at reducing the size of [OPENDAMIR database](http://open-data-assurance-maladie.ameli.fr/depenses/index.php)
along some particular axis to be able to manipulate it easily with `pandas`.
First of all, the database is converted to parquet format. 
Then, we define a set of function that use `dask` to load the parquet files
and group the monthly files according to particular indexes.
For instance here it groups the monthly database according to act category (`PRS_NAT_REF`)
and medical category (`PSE_ACT_CAT`), and sums total cost and total 
paid by the national healthcare system. 

The original dataset is around 5G per monthly file (not compressed format), 
that is approximately 60G for the whole year. 
After converting it to parquet, the reading with `dask` is very fast, reading 
each monthly file, grouping by by `PRS_NAT_REF`, aggregating and concatenating
all monthly files takes less than one minute for the whole dataset. 
The final dataset is 35K.
 
 I chose this axis as a mere example but this could be extended to the different
 variables present in the database.  
