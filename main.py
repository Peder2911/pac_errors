
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

import fire

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
def occurrence(conflictType = "either")-> pd.DataFrame:
    c = sqlite3.connect("data/pac.sqlite")

    pcountries = set(pd.read_sql("SELECT gwcode FROM predictions_2010_2050",c)["gwcode"].values)
    pcountries = {int(float(str(x).strip())) for x in pcountries}

    confVars = {
        "either": "either_actual",
        "major": "major_actual",
        "minor": "minor_actual"
    }
    confVar = confVars[conflictType]

    occurrence = fixDtypes(pd.read_sql(f"SELECT year,gwcode,{confVar} FROM acd",c))
    c.close()
    occurrence = occurrence[occurrence[confVar] != 0]

    span = np.arange(occurrence["year"].min(),occurrence["year"].max()+1)

    dfs = []
    for c in pcountries:
        df = pd.DataFrame({"year":span,"gwcode":c,"occ":0})
        df.index=span

        if c == 2:
            had = 1 if confVar in ["either","minor"] else 0
            df.at[df["year"] == 2001,"occ"] = 1
            dfs.append(df.reindex())
            continue

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
def discrep(conflictType = "either")-> pd.DataFrame:
    c = sqlite3.connect("data/pac.sqlite")
    
    predVars = {
        "either": "combined",
        "major": "major",
        "minor": "minor"
    }
    predVar = predVars[conflictType]

    predictions = fixDtypes(pd.read_sql("SELECT * FROM predictions_2010_2050",c))

    occ = occurrence(conflictType = conflictType)
    span = np.arange(occ["year"].min(),occ["year"].max()+1)

    predictions = predictions[predictions["year"].apply(lambda x: x in span)]
    predictions = predictions.merge(occ,on=["gwcode","year"],how="left")

    predictions["discrep"] = predictions["occ"] - predictions[predVar]
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


def main(conflictType = "either"):
    ds = discrep(conflictType = conflictType)
    ds = ds.merge(ccodes(),left_on="gwcode",right_on="gwn",how="left")
    shp = shapes()

    # =============================================
    plt.clf()

    meanSum = ds[["gwcode","discrep"]].groupby("gwcode").agg("mean").reset_index()
    meanSum.to_csv("/tmp/a.csv")
    #meanSum = meanSum.merge(ds[["gwcode","lastyear"]].drop_duplicates(),on="gwcode",how="inner")

    meanSum.to_csv("/tmp/b.csv")
    shp = shp.merge(meanSum,left_on="GWCODE",right_on="gwcode",how="left")

    vmin, vmax, vcenter = meanSum.discrep.min(), meanSum.discrep.max(), 0
    norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=vcenter, vmax=vmax)

    shp.plot(column="discrep",figsize=(15,8),legend=True,cmap="coolwarm",
        norm=norm,legend_kwds={'label': "Mean prediction discrepancy 2010-2018", 
            'orientation': "horizontal"})

    plt.savefig(f"maps/{conflictType}_mean.png")

    ds.to_csv("/tmp/d.csv")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fire.Fire(main)
