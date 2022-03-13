import pandas as Pandas
from joblib import Parallel, delayed
from os.path import isdir, join
from os import listdir
CHUNK_SIZE = 500000
N_JOBS = 16
# hello world
def read_from_list(path_list, n_jobs=N_JOBS, chunksize=CHUNK_SIZE, parallel_on_files=False, *args, **kwargs):
    if parallel_on_files:
        return Pandas.concat(Parallel(n_jobs=n_jobs)([delayed(lambda p: Pandas.read_csv(p, *args, **kwargs)) for p in path_list]), ignore_index=True)
    else:
        return Pandas.concat([Pandas.concat(
                Parallel(n_jobs=n_jobs)(map(delayed(lambda x: x), Pandas.read_csv(p, chunksize=chunksize, *args, **kwargs))))
                for p in path_list], ignore_index=True)

def read_from_file(path, n_jobs=N_JOBS, chunksize=CHUNK_SIZE, *args, **kwargs):
    return Pandas.concat(
            Parallel(n_jobs=n_jobs)(map(delayed(lambda x: x), Pandas.read_csv(path, chunksize=chunksize, *args, **kwargs))))

def get_list_files(path):
    return [join(path, file) for file in listdir(path) if not isdir(join(path, file)) and file.lower().endswith('.csv')]

# read csv file in parallel manner, in case a list of csv path is given, it reads them in parallel
def read_csv_parallel(path, n_jobs=N_JOBS, chunksize=CHUNK_SIZE, parallel_on_files=False, *args, **kwargs):
    if type(path) == str:
        if isdir(path):
            path = get_list_files(path)
            return read_from_list(path_list=path, n_jobs=n_jobs, chunksize=chunksize, *args, **kwargs)
        else:
            return read_from_file(path=path, n_jobs=n_jobs, chunksize=chunksize, *args, **kwargs)
    elif type(path) == list:
        return read_from_list(path_list=path, n_jobs=n_jobs, chunksize=chunksize, parallel_on_files=parallel_on_files, *args, **kwargs)
    else:
        raise Exception(f"path type has to be str or list of strings, not {type(path)}")

Pandas.read_csv_parallel = read_csv_parallel