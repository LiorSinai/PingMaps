#https://dev.maxmind.com/geoip/docs/databases/city-and-country
# get defaults locations for each country in the MaxMind database
# MaxMind falls back to these locations if they cannot pinpoint the address more precisely

import pandas as pd

DEFAULTS_FILENAME = "country_defaults.csv"


def make_country_default_table():
    meta_filename = "../../datasets/GeoLite2-City_20211012/GeoLite2-City-Locations-en.csv"

    df_meta = pd.read_csv(meta_filename)

    df_defaults = df_meta[
        pd.isna(df_meta["subdivision_1_iso_code"]) &
        pd.isna(df_meta["subdivision_1_name"]) &
        pd.isna(df_meta["subdivision_2_iso_code"]) &
        pd.isna(df_meta["subdivision_2_name"]) &
        pd.isna(df_meta["city_name"]) &
        pd.isna(df_meta["metro_code"])
        ]
    print(len(df_defaults))

    df_defaults.to_csv(DEFAULTS_FILENAME, index=False)


def set_default_coords():
    geo_filename = "../../datasets/GeoLite2-City_20211012/GeoLite2-City-Blocks-IPv4.csv"
    df_geo = pd.read_csv(geo_filename)
    df_defaults = pd.read_csv(DEFAULTS_FILENAME)

    df_defaults["latitude"] = 0
    df_defaults["long"] = 0

    for idx, row in df_defaults.iterrows():
        geoname_id = int(row["geoname_id"])
        df_local = df_geo[(df_geo["geoname_id"] == geoname_id) & (df_geo["registered_country_geoname_id"] == geoname_id)]
        country_name = df_defaults.loc[idx, "country_name"] 
        if len(df_local) == 0:
            print("No values found for id {:} for country {:}".format(geoname_id, country_name))
            print("")
        elif df_local["latitude"].nunique() > 1:
            print("Value counts id {:} for country {:}:".format(geoname_id, country_name))
            print(df_local["latitude"].value_counts())

            print("selecting most common value")
            lat = df_local["latitude"].value_counts().keys()[0] 
            lon = df_local[df_local["latitude"] == lat]["longitude"].values[0]
            print("")
        else:
            lat = df_local["latitude"].values[0]
            lon = df_local["longitude"].values[0]        
        df_defaults.loc[idx, "latitude"] = lat
        df_defaults.loc[idx, "longitude"] = lon
        
                    
    df_defaults.to_csv(DEFAULTS_FILENAME, index = False)


def get_defaults():
    df_defaults = pd.read_csv(DEFAULTS_FILENAME)
    defaults = [(row["latitude"], row["longitude"]) for idx, row in df_defaults.iterrows()]
    defaults = [(round(lat, 2), round(lon, 2)) for lat, lon in defaults]
    defaults.append((37, -97)) # United States was edited because of a law suite
    defaults.append((55.75,37.62)) # Moscow
    return defaults


if __name__ == '__main__':
    make_country_default_table()
    set_default_coords()
    get_defaults()
