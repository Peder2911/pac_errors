
import pandas as pd
import numpy as np
import sqlite3

from collections import defaultdict

#from sklearn.

c = sqlite3.connect("pac.sqlite")
acd = pd.read_sql("SELECT * FROM acd",c)

startyear = acd["year"].min()
endyear = acd["year"].max()
seriesLength = endyear - startyear
print(f"Starts in {startyear} and ends in {endyear}")

dataframes = []
for ctry in set(acd["gwcode"].values):
    series = dict() 
    for v in [
        "minor_actual",
        "major_actual",
        "either_actual"]:
        series[v] = pd.Series(np.zeros(seriesLength),dtype="int64")
        for idx,r in acd[acd["gwcode"]==ctry].iterrows():
            sidx = r["year"]-startyear-1
            series[v][sidx] = r[v]
    series["gwcode"] = pd.Series(np.full(seriesLength,ctry),
        dtype="int64")
    series["year"] = pd.Series(np.arange(startyear,endyear),
        dtype="int64")
    dataframes.append(pd.DataFrame(series))

occurrence= pd.concat(dataframes)
occurrence.to_csv("occ.csv")

predictions = pd.read_sql("SELECT * FROM predictions_2010_2050",c)

predictions["year"] = predictions["year"].astype(int)
predictions["gwcode"] = predictions["gwcode"].astype(int)
predictions["either"] = predictions["combined"]

print(predictions["gwcode"].head())
print(occurrence["gwcode"].head())

both = predictions.merge(occurrence,on=["gwcode","year"])
print(both.head())

for v in ["minor","major","either"]:
    both[v+"_discrep"] = both[v+"_actual"] - both[v]
both.to_csv("discrep.csv")

#print(seriesLength)
#for ctry in d.gwcode:

