# Based on https://erikbern.com/2015/04/26/ping-the-world.html

import random
import traceback
import subprocess
import time
import geolite2
import re

from multiprocessing.pool import ThreadPool

from defaults import get_defaults

DB_FILENAME = "../../datasets/GeoLite2-City_20211012/GeoLite2-City.mmdb"


def get_random_ip():
    return '.'.join(map(str, [random.randint(0, 255) for i in range(4)]))


def ping_ip(ip, lat, lon, idx, num_tries=1):
    try:
        print(idx, ip, lat, lon, '?')
        process = subprocess.run(['ping', '-n', str(num_tries), ip], capture_output=True)
        if process.returncode != 0:
            return None
        match = re.search("Average = (\d+)ms", process.stdout.decode())
        if match is None:
            return None
        tavg = int(match.group(1))/1000
        return ip, lat, lon, tavg
    except:
        traceback.print_exc()


def ping_random_ips(num_ips, logfile):
    logs = open(logfile, 'a')
    successes = 0
    matches = 0
    blacklist = get_defaults()
    geo = geolite2.MaxMindDb(filename=DB_FILENAME).reader()

    for i in range(num_ips):
        ip = get_random_ip()
        match = geo.get(ip)
        if match is None or not ('location' in match):
            continue
        matches += 1
        lat, lon = match["location"]["latitude"], match["location"]["longitude"]
        if (round(lat, 2), round(lon, 2)) in blacklist:
            continue
        result = ping_ip(ip, lat, lon, i, num_tries=1)
        print(result)
        if result is None:
            continue
        successes += 1
        ip, lat, lon, pingtime = result
        logs.write(str(lat) + "," + str(lon) + "," + str(pingtime) + "," + ip + "\n")
        logs.flush() # force write to file without delay
    logs.close()

    return matches, successes


def ping_random_ips_threaded(num_ips, logfile, nthreads=100):
    pool = ThreadPool(processes=nthreads)
    logs = open(logfile, 'a')
    geo = geolite2.MaxMindDb(filename=DB_FILENAME).reader()
    blacklist = get_defaults()
    successes = 0
    matches = 0

    def callback(result):
        nonlocal successes
        print(result)
        if result is None:
            return
        successes += 1
        ip, lat, lon, pingtime = result
        logs.write(str(lat) + "," + str(lon) + "," + str(round(pingtime, 5)) + "," + ip +"\n")
        logs.flush() # force write to file without delay

    for i in range(num_ips):
        ip = get_random_ip()
        match = geo.get(ip)
        if match is None or not ('location' in match):
            continue
        matches += 1
        lat, lon = match["location"]["latitude"], match["location"]["longitude"] 
        if (round(lat, 2), round(lon, 2)) in blacklist:
            continue
        pool.apply_async(ping_ip, args=(ip, lat, lon, i), kwds={"num_tries": 1}, callback=callback)

    pool.close()
    pool.join()
    logs.close()

    return matches, successes


if __name__ == '__main__':
    logfile = 'logs.txt'
    n = 10_000

    tstart = time.time()
    #matches, successes = ping_random_ips(n, logfile)
    matches, successes = ping_random_ips_threaded(n, logfile, nthreads=50)
 
    print("time take for {:d} calls: {:4f}".format(n, time.time() - tstart))
    print("match rate:   {:.3f}%".format(matches/n*100))
    print("success rate: {:.3f}%".format(successes/n*100))
    print("ping rate:    {:.3f}%".format(successes/matches*100))
