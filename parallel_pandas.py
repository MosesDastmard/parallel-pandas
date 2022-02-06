import pandas as Pandas
from joblib import Parallel, delayed


# read csv file in parallel manner, in case a list of csv path is given, it reads them in parallel
def read_csv_parallel(path, n_jobs=16, chunksize=500000, *args, **kwargs):
    if type(path) == str:
        return Pandas.concat(
            Parallel(n_jobs=n_jobs)(map(delayed(lambda x: x), Pandas.read_csv(path, chunksize=chunksize, *args, **kwargs))))
    if type(path) == list:
        return Pandas.concat([Pandas.concat(
            Parallel(n_jobs=n_jobs)(map(delayed(lambda x: x), Pandas.read_csv(p, chunksize=chunksize, *args, **kwargs))))
            for p in path], ignore_index=True)

Pandas.read_csv_parallel = read_csv_parallel