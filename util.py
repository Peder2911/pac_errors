
import logging
import os
import pandas as pd
import geopandas as gpd

from hashlib import md5

digest = lambda x: md5(x.encode()).hexdigest()

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

            path,outfile = os.path.split(fname)
            outfile,outext = os.path.splitext(outfile)

            ahash = digest(str(args) + str(kwargs))[:10]
            invocationName = os.path.join(path,(outfile+"_"+ahash+outext))

            if not os.path.exists(invocationName) or refresh:
                logging.info(f"Caching {invocationName}")

                dat = fn(*args,**kwargs)
                writefn(dat,invocationName)
                return dat
            else:
                logging.info(f"Using cache for {invocationName}")

                dat = readfn(invocationName)
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
