
import pandas as pd
from matplotlib import pyplot as plt

discrep = pd.read_csv("discrep.csv")
print(discrep.groupby("gwcode").size().rename("count").reset_index().groupby("count").size())
codes = pd.read_csv("gwcodes.csv")
discrep = discrep.merge(codes,on="gwcode").drop_duplicates()

plt.clf()
plt.hist(discrep["either_discrep"])
plt.savefig("/tmp/d.png")

discrep.to_csv("/tmp/d.csv")
discrep[["name","minor_discrep"]].groupby("name").agg("mean").to_csv("/tmp/groups.csv")
