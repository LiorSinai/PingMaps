import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import cartopy.crs as ccrs
import cartopy.feature as cfeature


def make_mesh(dataframe, step=5, maxval=10):
    latitude = np.arange(-90, 90, step)
    longitude = np.arange(-180, 180, step)
    nrows, ncols = len(latitude), len(longitude)
    values = np.zeros((nrows, ncols))
    counts = np.zeros((nrows, ncols))

    i0 = latitude[0] + step/2
    j0 = longitude[0] + step/2 
    for idx, row in dataframe.iterrows():
        lat  = row["latitude"]
        long = row["longitude"]
        i = min(max(int(np.ceil((lat - i0)/step)), 0), nrows - 1) 
        j = min(max(int(np.ceil((long - j0)/step)), 0), ncols - 1)
        if not np.isnan(row["pingtime"]):
            values[i][j] += row["pingtime"]
            counts[i][j] += 1

    mesh = np.where(counts > 0, values/counts, np.nan)
    mesh[mesh > maxval] = maxval
    return latitude, longitude, mesh
    

def plot_ping_map(logs_path, std_outlier=3):
    fig = plt.figure(figsize=(10, 5))
    source_proj = ccrs.Robinson()
    ax = fig.add_subplot(1, 1, 1, projection=source_proj)

    dataframe = pd.read_csv(logs_path, header=0)
    maxval = dataframe["pingtime"].mean() + std_outlier * dataframe["pingtime"].std()
    maxval = np.ceil(maxval * 10)/10  # round up
    Y, X, Z = make_mesh(dataframe, step=1, maxval=maxval)

    canvas = plt.pcolormesh(X, Y, Z, cmap='jet', transform=ccrs.PlateCarree())
    pos = ax.get_position()
    cbar_ax = fig.add_axes([pos.x1 + 0.01, pos.y0, 0.015, pos.height])
    cbar = plt.colorbar(canvas, cax=cbar_ax)
    cbar.set_label("ping time (s)")

    ax.coastlines(linewidth=0.25)
    ax.add_feature(cfeature.BORDERS, linestyle='-', linewidth=0.2)

    plt.show()


if __name__ == '__main__':
    logs_path = "logs4.txt"
    plot_ping_map(logs_path)
