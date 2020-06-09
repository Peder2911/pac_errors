
import pandas as pd
import geopandas as gpd

import numpy as np
import sqlite3

from util import cache

from collections import defaultdict

from matplotlib import pyplot as plt

from matplotlib import colors

import subprocess
import logging

import os

def fixDtypes(df:pd.DataFrame)->pd.DataFrame:
    for v in df.columns:
        if v not in ["minor","major","combined"]:
            df[v] = df[v].apply(lambda x: int(float(str(x).strip())))
    return df

@cache("cache/cc.csv")
def ccodes() -> pd.DataFrame:
    fnm = f"/tmp/{os.getpid()}.csv"
    
    rcmd = f"""
        write.csv(countrycode::codelist[c('gwn','country.name.en','iso3c')],'{fnm}')
    """
    op = subprocess.check_output(["R","-e",rcmd])
    d = pd.read_csv(fnm)
    os.unlink(fnm)
    return d.reindex() 
@cache("cache/occurrence.csv")
def occurrence()-> pd.DataFrame:
    c = sqlite3.connect("data/pac.sqlite")

    pcountries = set(pd.read_sql("SELECT gwcode FROM predictions_2010_2050",c)["gwcode"].values)
    pcountries = {int(float(str(x).strip())) for x in pcountries}

    occurrence = fixDtypes(pd.read_sql("SELECT year,gwcode,either_actual FROM acd",c))
    c.close()

    span = np.arange(occurrence["year"].min(),occurrence["year"].max()+1)
    #occurrence = occurrence[occurrence["year"].apply(lambda x: x in span)]

    dfs = []
    for c in pcountries:
        df = pd.DataFrame({"year":span,"gwcode":c,"occ":0})
        df.index=span
        if c in set(occurrence["gwcode"].values):
            occ = occurrence[occurrence["gwcode"] == c].sort_values("year")
            occ.index = occ["year"]
            for idx,r in occ.iterrows():
                df.at[idx,"occ"] = 1
        else:
            pass
        dfs.append(df.reindex())

    return fixDtypes(pd.concat(dfs))

@cache("cache/discrep.csv")
def discrep()-> pd.DataFrame:
    c = sqlite3.connect("data/pac.sqlite")

    predictions = fixDtypes(pd.read_sql("SELECT * FROM predictions_2010_2050",c))

    occ = occurrence()
    span = np.arange(occ["year"].min(),occ["year"].max()+1)

    predictions = predictions[predictions["year"].apply(lambda x: x in span)]
    predictions = predictions.merge(occ,on=["gwcode","year"],how="left")

    predictions["discrep"] = predictions["occ"] - predictions["combined"]
    return predictions

@cache("cache/last.csv")
def last()-> pd.DataFrame:
    occ = occurrence()
    hasOcc = occ[occ["occ"] > 0]
    noOcc = occ[occ["gwcode"].apply(lambda x: x not in set(hasOcc["gwcode"].values))]

    return pd.concat([
        hasOcc[["gwcode","year"]].groupby("gwcode")["year"].agg("max").rename("lastyear").reset_index(),
        pd.DataFrame({"lastyear":-1,"gwcode":[*set(noOcc["gwcode"].values)]})
    ])

@cache("cache/cshp.geojson")
def shapes()-> gpd.GeoDataFrame:
    d = gpd.read_file("data/cshapes.shp")

    d = d[
        (d["GWEYEAR"] == 2016)
    ]
    return d


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ds = discrep()
    ds = ds.merge(ccodes(),left_on="gwcode",right_on="gwn",how="left")
    ds = ds.merge(last(),on="gwcode",how="left")
    shp = shapes()
    

    # =============================================
    plt.clf()

    meanSum = ds[["gwcode","discrep"]].groupby("gwcode").agg("mean").reset_index()
    meanSum.to_csv("/tmp/a.csv")
    meanSum = meanSum.merge(ds[["gwcode","lastyear"]].drop_duplicates(),on="gwcode",how="inner")
    meanSum["classLo"] = (meanSum["lastyear"]<2000) & (meanSum["discrep"]<-0.05)
    meanSum["classHi"] = (meanSum["discrep"]>0.5)

    meanSum.to_csv("/tmp/b.csv")
    shp = shp.merge(meanSum,left_on="GWCODE",right_on="gwcode",how="left")

    vmin, vmax, vcenter = meanSum.discrep.min(), meanSum.discrep.max(), 0
    norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=vcenter, vmax=vmax)

    shp.plot(column="discrep",figsize=(15,8),legend=True,cmap="coolwarm",
        norm=norm,legend_kwds={'label': "Mean prediction discrepancy 2010-2018", 
            'orientation': "horizontal"})

    plt.savefig("maps/mean.png")

    for v in ["classHi","classLo"]:
        plt.clf()
        shp.plot(column=v,figsize=(15,8))
        plt.savefig(f"maps/{v}.png")

    # =============================================

