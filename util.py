
import logging
import os
import pandas as pd
import geopandas as gpd

def cache(fname,refresh=False):
    disp = {
        ".csv": (pd.read_csv,lambda x,dest: x.to_csv(dest,index=False)),
        ".xlsx": (pd.read_excel,lambda x,dest: x.to_excel(dest)),
        ".geojson": (gpd.read_file ,lambda x,dest: x.to_file(dest,driver="GeoJSON"))
    }

    def wrapper(fn):
        _,ext = os.path.splitext(fname)

        def inner(*args,**kwargs):
            readfn,writefn = disp[ext]

            if not os.path.exists(fname) or refresh:
                logging.info(f"Caching {fname}")

                dat = fn()
                writefn(dat,fname)
                return dat
            else:
                logging.info(f"Using cache for {fname}")

                dat = readfn(fname)
                return dat
        return inner
    return wrapper

if __name__ == "__main__":

    logging.basicConfig(level = logging.INFO)

    try:
        os.unlink("/tmp/c.csv")
    except:
        pass

    @cache("/tmp/c.csv")
    def doSomething():
        return pd.DataFrame([[1,2,3],[4,5,6]])

    a = doSomething()
    b = doSomething()
    assert (a.values == b.values).all()

    try:
        os.unlink("/tmp/c.csv")
    except:
        pass
