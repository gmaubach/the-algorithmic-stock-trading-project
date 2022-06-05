#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
__program__     = btc_daily_prices.py
__author__      = "Georg Maubach"
__authors__     = "Georg Maubach"
__copyright__   = "(C)opyright 2022, The Algorithmic Stock Trading Project"
__credits__     = ["Boris Verkhovskiy", "Black Badger", "Gifted Grebe", "Zach Bobbitt", "congusbongus", "cs95", "Data Science Parichay"] 
__license__     = "GPL"
__maintainer__  = "Georg Maubach"
__contact__     = "g.maubach@gmx.de"
__email__       = "g.maubach@gmx.de"
__created__     = "2022-05-31"
__updated__     = "2022-06-04"
__status__      = "Development"
__version__     = "0.0.1"
__interpreter__ = "Python 3.8.10"
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
    "time_open_unix", "price_open", "price_high", "price_low", "price_close",
    "volume", "time_close_unix", "quote_asset_volume", "base_asset_volume",
    "ignore"
    )
    
DATA_DIR: Final[str] = "/home/gmaubach/Programming/tastp/data/"

#=== VAR ===

#=== CLASS ===

#=== FUNCTION ===

def get_binance_hist_prices(
    domain: str = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/",
    base: str = "BTCBUSD-1m-",
    date: str = START_DATE,
    suffix: str = ".zip") -> pd.DataFrame:
    if __debug__: print("START: get_binance_hist_prices()")

    # Download from web
    url = domain + base + date + suffix
    print("URL of Zipfile: ", repr(url))

    content = requests.get(url)

    z = ZipFile(BytesIO(content.content))
    print(f"Files in ZipFile: {z.namelist()}")

    # Unzip content on the fly in memory
    with z.open(z.namelist()[0], 'r') as f:
        df = pd.read_csv(
            f,
            header = None,
            names = TABLE_HEADER,
            index_col = False
            )

    df.index.name = "serial"

    if __debug__: print("END: get_binance_hist_prices()")
    return(df)

def generate_day_sequence(
    start_date: str = START_DATE,
    end_date: str = END_DATE) -> list:
    if __debug__: print("START: generate_day_sequence()")
    
    dates = pd.date_range(
        start = start_date,
        end = end_date).strftime("%Y-%m-%d").to_list()
    
    if __debug__: print("END: generate_day_sequence()")
    return(dates)

def get_binance_hist_price_series(
    domain: str = "https://data.binance.vision/data/spot/daily/klines/BTCBUSD/1m/",
    base: str = "BTCBUSD-1m-",
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    suffix: str = ".zip") -> pd.DataFrame:
    if __debug__: print("START: get_binance_hist_price_series()")

    dates = generate_day_sequence(
        start_date = start_date,
        end_date = end_date)

    df = pd.DataFrame()
    
    for date in dates:
        temp = get_binance_hist_prices(
            domain = domain,
            base = base,
            date = date,
            suffix = suffix)
        df = df.append(temp)

    print(f"Memory usage of dataframe {df.memory_usage().sum():,.0f} Bytes")

    if __debug__: print("END: get_binance_hist_price_series()")
    return(df)

def prepare_binance_prices(
    data: pd.DataFrame
    ) -> pd.DataFrame:
    if __debug__: print("START: prepare_binance_prices()")

    df = data # %PERF% If performance is an issue do without copying the data to a new object.
    
    # Convert UNIX datetime with 13 digits and milliseconds into human readable format
    df["time_open_readable"] = df["time_open_unix"].astype("datetime64[ms]").astype("datetime64[s]")
    df["time_close_readable"] = df["time_close_unix"].astype("datetime64[ms]").astype("datetime64[s]")

    # Extract year, month, day, hour, minute from datetime objects
    # Data redundance is generated on purpose.
    ## time_open
    df["time_open_year"] = df["time_open_readable"].dt.year
    df["time_open_month"] = df["time_open_readable"].dt.month
    df["time_open_day"] = df["time_open_readable"].dt.day
    df["time_open_hour"] = df["time_open_readable"].dt.hour
    df["time_open_minute"] = df["time_open_readable"].dt.minute
    ## time_close
    df["time_close_year"] = df["time_close_readable"].dt.year
    df["time_close_month"] = df["time_close_readable"].dt.month
    df["time_close_day"] = df["time_close_readable"].dt.day
    df["time_close_hour"] = df["time_close_readable"].dt.hour
    df["time_close_minute"] = df["time_close_readable"].dt.minute

    print(f"Memory usage of dataframe {df.memory_usage().sum():,.0f} Bytes")
    
    if __debug__: print("START: prepare_binance_prices()")
    return(df)

def save_binance_price_series_to_csv(
    data: pd.DataFrame,
    path: Path = DATA_DIR,
    filename: Path = "binance_BTCDUSD_price_series",
    suffix: Path = ".csv") -> None:
    if __debug__: print("START: save_binance_price_series_to_csv()")

    file =  path + filename + suffix
    data.to_csv(file)

    if __debug__: print("END: save_binance_price_series_to_csv()")
    return(None)

def save_binance_price_series_to_excel(
    data: pd.DataFrame,
    path: Path = DATA_DIR,
    filename: Path = "binance_BTCDUSD_price_series",
    suffix: Path = ".xlsx") -> None:
    if __debug__: print("START: save_binance_price_series_to_excel()")
    
    file =  path + filename + suffix

    data.to_excel(
        file,
        sheet_name = "Sheet1"
        )

    if __debug__: print("END: save_binance_price_series_to_excel()")
    return(None)

def save_binance_price_series_to_parquet(
    data: pd.DataFrame,
    path: Path = DATA_DIR,
    filename: Path = "binance_BTCDUSD_price_series",
    suffix: Path = ".parquet") -> None:
    if __debug__: print("START: save_binance_price_series_to_parquet()")
    
    file =  path + filename + suffix

    data.to_parquet(file)

    if __debug__: print("END: save_binance_price_series_to_parquet()")
    return(None)

#=== MAIN ===

if  __name__ == "__main__":

    # prices = get_binance_hist_prices()
    prices = get_binance_hist_price_series(start_date = START_DATE, end_date = END_DATE)
    prices = prepare_binance_prices(data = prices)
    print(prices.info())
    print(prices.describe())
    # save_binance_price_series_to_csv(data = prices)
    # save_binance_price_series_to_excel(data = prices)
    save_binance_price_series_to_parquet(data = prices)

#=== REFERENCE ===

# *01 = Prices = https://data.binance.vision/?prefix=data/spot/daily/klines/BTCBUSD/1m/
# *02 = Column Names = https://github.com/binance/binance-public-data/
# *03 = Unzip file on the fly = https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk (Boris Verkhovskiy)
# *04 = Date List = https://www.codegrepper.com/code-examples/python/generate+python+date+list (Black Badger)
# *05 = Date List Formatting = https://www.codegrepper.com/code-examples/python/subtract+one+hour+from+datetime+python (Gifted Grebe)
# *06 = Adding Dataframes = https://www.statology.org/pandas-add-row-to-dataframe/#:~:text=You%20can%20use%20the%20df,to%20end%20of%20DataFrame%20df. (Zach Bobbitt)
# *07 = Constants in Python = https://stackoverflow.com/questions/802578/final-keyword-equivalent-for-variables-in-python (congusbongus)
# *08 = Python Typing = https://docs.python.org/3/library/typing.html (Python Software Foundation)
# *09 = Writing Parquet Files = https://pandas.pydata.org/pandas-docs/version/1.1/reference/api/pandas.DataFrame.to_parquet.html (pydata.org)
# *10 = Convert UNIX datetime = https://stackoverflow.com/questions/34883101/pandas-converting-row-with-unix-timestamp-in-milliseconds-to-datetime (cs95)
# *11 = Extract Year from a datetime column = https://datascienceparichay.com/article/pandas-extract-year-from-datetime-column (Data Science Parichay)

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
