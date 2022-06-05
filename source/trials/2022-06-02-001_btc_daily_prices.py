#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Program:     btc_daily-prices.py
Author:      Georg Maubach
Created:     2022-05-31
Updated:     2022-06-02
Interpreter: Python 3.8.10
"""

#=== USES ===
#--- Standard ---
from io import BytesIO
from typing import Final
from pathlib import Path
import requests
from zipfile import ZipFile

#--- 3rd Party ---
import pandas as pd
import parquet

#--- MyOwn ---

#=== CONST ===

START_DATE: Final[str] = "2022-05-01"
END_DATE: Final[str] = "2022-05-31"

TABLE_HEADER: Final[tuple] = (
    "time_open", "price_open", "price_high", "price_low", "price_close",
    "volume", "time_close", "quote_asset_volume", "base_asset_volume", "ignore"
    )

#=== VAR ===

#=== CLASS ===

#=== FUNCTION ===

def get_binance_hist_prices(
    domain: str = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/",
    base: str = "BTCBUSD-1m-",
    date: str = "2022-05-31",
    suffix: str = ".zip") -> pd.DataFrame:

    # Download from web
    url = domain + base + date + suffix
    print("URL of Zipfile. ", repr(url))

    content = requests.get(url)

    z = ZipFile(BytesIO(content.content))
    print(f"Files in ZipFile: {z.namelist()}")

    # Unzip content
    with z.open(z.namelist()[0], 'r') as f:
        ts = pd.read_csv(
            f,
            header = None,
            names = TABLE_HEADER
            )
    
    return(ts)

def generate_day_sequence(
    start_date: str = "2022-05-01",
    end_date: str = "2022-05-31") -> list:

    dates = pd.date_range(
    start = start_date,
    end = end_date).strftime("%Y-%m-%d").to_list()

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

    df = pd.DataFrame(columns = TABLE_HEADER)
    for date in dates:
        temp = get_binance_hist_prices(
            domain = domain,
            base = base,
            date = date,
            suffix = suffix)
        df = df.append(temp)

    return(df)

def save_binance_price_series(
    data: pd.DataFrame,
    path: Path = "/home/gmaubach/Programming/Data/",
    filename: Path = "binance_BTCDUSD_price_series.csv") -> None:

    file =  path + filename
    data.to_csv(file)

    return(None)

#=== MAIN ===

if  __name__ == "__main__":
    # prices = get_binance_hist_prices()
    prices = get_binance_hist_price_series(start_date = "2022-05-01", end_date = "2022-05-02")
    print(prices.info())
    print(prices.describe())
    save_binance_price_series(data = prices)

#=== REFERENCE ===

# *01 = Prices = https://data.binance.vision/?prefix=data/spot/daily/klines/BTCBUSD/1m/
# *02 = Column Names = https://github.com/binance/binance-public-data/
# *03 = Unzip file on the fly =  https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
# *04 = Date List = https://www.codegrepper.com/code-examples/python/generate+python+date+list
# *05 = Date List Formatting = https://www.codegrepper.com/code-examples/python/subtract+one+hour+from+datetime+python
# *06 = Adding Dataframes = https://www.statology.org/pandas-add-row-to-dataframe/#:~:text=You%20can%20use%20the%20df,to%20end%20of%20DataFrame%20df.
# *07 = Constants in Python = https://stackoverflow.com/questions/802578/final-keyword-equivalent-for-variables-in-python
# *08 = Python Typing = https://docs.python.org/3/library/typing.html
# *09 = Writing Parquet Files = https://pandas.pydata.org/pandas-docs/version/1.1/reference/api/pandas.DataFrame.to_parquet.html

#== FURTHER INFO ===
"""
**??** The problem is: I can not read the values in the columns. How can I convert the values into human readable format?
**!!** You can read the data directly with what you see in the CSV file except the time. The time is UNIX type of time format which come with 13 digits. You could google on it to get the convert tool for the UNIX time format.
**!!** If the open time is stated with 12pm UNIX time format. The open time is 12pm.
**!!** Yes you understand it correctly. As this is 13 digit timestamp format, you could look for UNIX time format for milliseconds.
**!!** The history data is about UTC time.
Source: Chat with Binance support on Thu 2022-06-02 08:05
"""

# EOF .
