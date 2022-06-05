#!/usr/bin/env python
# -*- coding: utf-8 -*-

#=== USES  ===

from io import BytesIO
import requests
import pandas as pd
from pathlib import Path
from zipfile import ZipFile

"""
date_start = "2022-05-01"
date_end = "2022-05-31"
date = "2022-05-31"
dates = pd.date_range(
    start = date_start,
    end = date_end).strftime("%Y-%m-%d").to_list()
domain = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/"
trunk = "BTCBUSD-1m-"
suffix = ".zip"
"""

#=== CONST ===

TABLE_NAMES: Final = (
    "time_open", "price_open", "price_high", "price_low", "price_close",
    "volume", "time_close", "quote_asset_volume", "base_asset_volume", "ignore"
    )

#=== CLASS ===

#=== FUNCTION ===

def get_binance_hist_prices(
    domain: str = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/",
    base: str = "BTCBUSD-1m-",
    date: str = "2022-05-31",
    suffix: str = ".zip") -> pd.DataFrame:

    # Download from web
    url = domain + base + date + suffix
    print("URL to Zipfile. ", repr(url))

    content = requests.get(url)

    z = ZipFile(BytesIO(content.content))
    print(f"Files in ZipFile: {z.namelist()}")

    # Unzip content
    with z.open(z.namelist()[0], 'r') as f:
        ts = pd.read_csv(
            f,
            header = None,
            names = TABLE_NAMES
            )
    
    return(ts)

def generate_day_sequence(
    start_date: str = "2022-05-01",
    end_date: str = "2022-05-31") -> list[str]:

    dates = pd.date_range(
    start = date_start,
    end = date_end).strftime("%Y-%m-%d").to_list()

    return(dates)

def get_binance_hist_price_series(
    domain: str = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/",
    base: str = "BTCBUSD-1m-",
    start_date: str = "2022-05-01",
    end_date: str = "2022-05-31",
    suffix: str = ".zip") -> pd.DataFrame:

    dates = generate_day_sequence(
        start_date = start_date,
        end_date = end_date)

    if 
    df = get_binance_hist_prices(
       domain = domain,
       base = base;
       date = ,
       suffix = suffix)
            
#=== MAIN ===

if  __name__ == "__main__":
    prices = get_binance_hist_prices()
    print(prices.info())
    print(prices.describe())

# Sources
# *1 = Prices = https://data.binance.vision/?prefix=data/spot/daily/klines/BTCBUSD/1m/
# *2 = Column Names = https://github.com/binance/binance-public-data/
# *3 = Unzip file on the fly =  https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
# *4 = Date List = https://www.codegrepper.com/code-examples/python/generate+python+date+list
# *5 = Date List Formatting = https://www.codegrepper.com/code-examples/python/subtract+one+hour+from+datetime+python

# EOF .
