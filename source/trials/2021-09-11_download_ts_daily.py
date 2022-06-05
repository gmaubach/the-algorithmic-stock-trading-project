#!/usr/bin/env python
# -*- coding: utf-8 -*-
#--------1---------2---------3---------4---------5---------6---------7----|

from datetime import datetime
import os.path
import pandas as pd
import sqlite3
import sys
import time
from typing import Final

MY_MODULES: Final = "/home/gmaubach/Programming/StockTradingApp2/Source"
sys.path.append(MY_MODULES)
from read_symbols_from_company_list import read_symbols_from_company_list

def download_ts_daily(
    apikey: str = None,
    db_file: str = None,
    symbols: list = [],
    start_date: str = "YYYY-MM-DD",
    end_date: str = "YYYY-MM-DD"
    ) -> None :
    
    r"""
    
    Download time series for symbols from AlphaVantage.
    
    Time series data is downloaded and stored in the database.
    
    Parameters
    ----------
    apikey : string, required
        API Key of AlphaVantage Stock API
        (https://www.alphavantage.co/documentation/)
    db_file : string, required
        Path and filename of the database.
    symbols : list, required
        List of symbols.
    start_date: string, required
        The first date for downloading data.
    end_date: string, required
        The last date for downloading data.    
    
    Warnings
    --------
    SQL-Injection-Vulnerability :
        Variable "cmd" to assemble SQL statement is vulnerable
        to SQL injections. Change code before using on the internet
        (see also:
         - https://docs.python.org/3/library/sqlite3.html
         - https://realpython.com/prevent-python-sql-injection/)
    """
    
    if __debug__:
       print(f"apikey: {apikey}")
       print(f"db_file: {db_file}")
       print(f"symbols: {symbols}")
       print(f"start_date: {start_date}")
       print(f"end_date: {end_date}")
    
    # -- Checks --
    # Check API key.
    if apikey == None:
        raise ValueError(
            f"Value for API key not given!\n")
    
    # Check database.
    if not(os.path.isfile(os.path.join(db_file))):
        raise IOError(
            f"Database not found!\n")

    # Check database table.
    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='ts_daily'")
        if not (cursor.fetchone()[0] == 1):
            connection.close()
            raise Error(
                f"Table 'ts_daily' for time series data not found in database!")
    
    # Check symbols.
    if not isinstance(symbols, list) or len(symbols) == 0:
        raise ValueError(f"Wrong values for argument 'symbols'!")

    # Check symbols in database.
    symbols_in_database = read_symbols_from_company_list(db_file)
    missing_symbols = [symbol for symbol in symbols if symbol not in symbols_in_database]
    if (len(missing_symbols) > 0):
        print(f"Missing symbols: {missing_symbols}")
        raise ValueError(
            f"Some symbols in argument 'symbols' are missing in database!\n")
                
    # Check dates.
    if (start_date == "YYYY-MM-DD" or
        end_date == "YYYY-MM-DD"):
        raise ValueError(
            f"Wrong value type for argument 'start_date' and/or 'end_date'!\n")

    if (datetime.strptime(start_date, "%Y-%m-%d") >
       datetime.strptime(end_date, "%Y-%m-%d")):
       raise ValueError(
           f"start_date is earlier than end_date!\n")
           
    # -- Download --
    # Get time series for each symbol.
    df_symbols = pd.DataFrame({"Symbol": symbols})
    for symbol in df_symbols["Symbol"]:
        print(f"Downloading data for {symbol} ... ")
        URL = (
            f"https://www.alphavantage.co/"
            f"query?function=TIME_SERIES_DAILY"
            f"&symbol={symbol}&"
            f"outputsize=full&"
            f"datatype=csv"
            f"&apikey={apikey}")
        if __debug__:
            print(URL)
        
        # ts = time series.
        ts_daily = pd.read_csv(URL)
        
        # Add symbol for all rows
        ts_daily["symbol"] = [symbol] * len(ts_daily)       
    
        if __debug__:
            print(ts_daily.columns.values)
            print(ts_daily.head())
            print(ts_daily.tail())
            print(f"Object Memory Usage: "
                f"{sys.getsizeof(ts_daily)/1024:.0f} kB.")
                
        # Delete time series data for the symbol
        # for the requested period indicated by
        # start_date and end_date.
        cmd = [
            f"DELETE FROM ts_daily ",
            f"WHERE symbol = '{symbol}' AND ",
            f"( timestamp BETWEEN '{start_date}' AND '{end_date}' );"
            ]
        cmd = "".join(cmd)
        if __debug__:
            print(f"SQL Statement: {cmd}")
            
        with sqlite3.connect(db_file) as connection:
            cursor = connection.cursor()
            cursor.execute(cmd)
        connection.commit()
        
        if __debug__:
            cmd = [
                f"SELECT * FROM ts_daily ",
                f"WHERE symbol = '{symbol}' AND ",
                f"( timestamp BETWEEN '{start_date}' AND '{end_date}' );"
                ]
            cmd = "".join(cmd)
            cursor.execute(cmd)
            connection.commit()
            print(f"SQL Statement: {cmd}")
            print(f"Data left in table for {symbol} between {start_date} and {end_date}: {cursor.fetchall()}")
            
            cmd = "SELECT DISTINCT symbol from ts_daily;"
            cursor.execute(cmd)
            connection.commit()
            symbols_left_in_database = [row[0] for row in cursor.fetchall()]
            print(f"Symbols left in database: {symbols_left_in_database}")

        connection.close()
        
        # Select the time frame from the time series download.
        try:
            ts_daily = ts_daily[
                (ts_daily["timestamp"] >= start_date) &
                (ts_daily["timestamp"] <= end_date)
                ]
            # ts_daily contains only data for selected symbol,
            # thus a selection on "symbol" is not necessary.
            
            # Insert the new data into the database.
            with sqlite3.connect(db_file) as connection:
                result = ts_daily.to_sql("ts_daily", connection, if_exists = "append", index = False)
                if __debug__:
                    print(f"Data for {symbol} between {start_date} and {end_date} written to database with result: {result}!")
            connection.commit()
            connection.close()
        except:
            print(f"No time series data avaiable for {symbol}.")
            pass
        
        # Wait to not get rejected by the API.
        if __debug__:
            print(f"Waiting 60 sec ...")
        time.sleep(60) 

if __name__ == "__main__":

    # TODO Modul von außen ansprechparbar machen.
    # TODO Benannte Parameter einführen und auswerten 
    
    APIKEY: Final = "UJST38QOGCGQWZWC"
    DB_FILE: Final = "/home/gmaubach/Programming/StockTradingApp2/Data/StockTradingDB.sqlite"
    
    symbol_list = read_symbols_from_company_list(DB_FILE)
    symbol_list = [symbol for symbol in symbol_list if symbol.upper() != "XXXXX"]
    # symbol_list = ["ATVI",]
    if __debug__:
        print(f"Type of 'symbol': {type(symbol_list)}")

    download_ts_daily(
        apikey = APIKEY,
        db_file = DB_FILE,
        symbols = symbol_list,
        start_date = "2000-01-01",
        end_date = "2021-12-31")        

# EOF.
