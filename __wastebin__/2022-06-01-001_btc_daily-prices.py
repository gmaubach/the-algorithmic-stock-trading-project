#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import BytesIO
import requests
import pandas as pd
from pathlib import Path
from zipfile import ZipFile

# URL: https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/
# File: BTCBUSD-1m-2022-05-29.zip
# Columns: https://github.com/binance/binance-public-data/

# Download from web
iso_date = "2022-05-30"
domain = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/"
file_name = "BTCBUSD-1m-2022-05-30.zip"
url = domain + file_name
# url = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/" + "BTCBUSD-1m-2022-05-30.zip"
print(repr(url))

content = requests.get(url)
# print(content.url)
# print(content.text)

f = ZipFile(BytesIO(content.content))
print(f"Files in ZipFile: {f.namelist()}")

# Unzip content
with f.open(f.namelist()[0], 'r') as g:
    btc = pd.read_csv(
        g,
        header = None,
        names = [
            "time_open", "price_open", "price_high", "price_low", "price_close",
            "volume", "time_close", "quote_asset_volume", "base_asset_volume", "ignore"]
        )

print(btc.info())
print(btc.describe())

# Sources
# *1: https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk

# EOF .
