import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import cartopy.crs as ccrs
import cartopy.feature as cfeature

from defaults import get_defaults


def make_mesh(filename, step=5):
    dataframe = pd.read_csv(filename)
    dataframe = dataframe[["network", "latitude", "longitude"]]

    latitude = np.arange(-90, 90, step)
    longitude = np.arange(-180, 180, step)
    nrows, ncols = len(latitude), len(longitude)
    counts = np.zeros((nrows, ncols))

    powers = [2**(32 - x) for x in range(33)]

    blacklist = set(get_defaults())
    skipped = 0

    i0 = latitude[0] + step/2
    j0 = longitude[0] + step/2 
    for idx, row in dataframe.iterrows():
        lat  = row["latitude"]
        long = row["longitude"]
        if pd.isna(lat) or pd.isna(long):
            skipped += 1
            continue
        if (round(lat, 2), round(long, 2)) in blacklist:
            skipped += 1
            continue

        i = min(max(int(np.ceil((lat - i0)/step)), 0), nrows - 1) 
        j = min(max(int(np.ceil((long - j0)/step)), 0), ncols - 1)

        ip0, block = row["network"].split("/")
        counts[i][j] += powers[int(block)]
        if (idx % 100_000 == 0):
            print(idx, "{:.4f}".format(idx/len(dataframe)*100))

    np.save(f"count_CDIR_{step}deg.npy", counts)
    print(f"skipped {skipped} entries")

    return latitude, longitude, counts
    

def plot_ping_map(filename):
    Z = np.load(filename)
    step = Z.shape[0]//180
    Y = np.arange(-90, 90, step)
    X = np.arange(-180, 180, step)
    Z[Z == 0] = np.nan

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.Robinson())

    canvas = plt.pcolormesh(X, Y, Z, cmap='jet', transform=ccrs.PlateCarree())
    pos = ax.get_position()
    cbar_ax = fig.add_axes([pos.x1 + 0.01, pos.y0, 0.015, pos.height])
    cbar = plt.colorbar(canvas, cax=cbar_ax)
    cbar.set_label("frequencies")

    ax.coastlines(linewidth=0.25)
    ax.add_feature(cfeature.BORDERS, linestyle='-', linewidth=0.2)

    plt.show()


if __name__ == '__main__':
    filename = "../../datasets/GeoLite2-City_20211012/GeoLite2-City-Blocks-IPv4.csv"
    make_mesh(filename, step=1)

    filename = "count_CDIR_1deg_no_defaults.npy"
    plot_ping_map(filename)
