import time
import pandas as pd

from multiprocessing.pool import ThreadPool

from defaults import get_defaults
from ping import ping_ip


def ping_db_ips_threaded(dataframe, filename, nthreads=100):
    pool = ThreadPool(processes=nthreads)

    dataframe_old = pd.read_csv(filename, header=0)
    ips_old = set(dataframe_old["ip"])
    START_IDX = 0 

    logs = open(filename, 'a')
    successes = 0
    attempts = 0

    blacklist = set(get_defaults())

    def callback(result):
        nonlocal successes
        print(result)
        if result is None:
            return
        successes += 1
        ip, lat, lon, pingtime = result
        logs.write(str(lat) + "," + str(lon) + "," + str(pingtime) + "," + ip +"\n")
        logs.flush() # force write to file without delay

    for idx, row in dataframe.iterrows():
        if idx < START_IDX or row["ip"] in ips_old:
          continue
        attempts += 1
        lat, lon = row["latitude"], row["longitude"]
        ip = row["ip"]
        if pd.isna(lat) or pd.isna(lon) or ((round(lat, 2), round(lon, 2)) in blacklist):
            continue
        pool.apply_async(ping_ip, args=(ip, lat, lon, idx), kwds={"num_tries": 4}, callback=callback)

    pool.close()
    pool.join()
    logs.close()

    return successes, attempts


if __name__ == '__main__':
    infile = "logs.txt"
    outfile = "logs4.txt"
    
    df = pd.read_csv(infile, header=0)

    tstart = time.time()
    successes, attempts = ping_db_ips_threaded(df, outfile, nthreads=100)
    print("time take for {:d} calls: {:4f}".format(attempts, time.time() - tstart))
    print("success rate: {:.3f}%".format(successes/attempts * 100))
