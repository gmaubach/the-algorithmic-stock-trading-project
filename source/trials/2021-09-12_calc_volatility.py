#!/usr/bin/env python
# -*- coding: utf-8 -*-
#--------1---------2---------3---------4---------5---------6---------7----|

# == INCLUDE ==
from datetime import datetime
import numpy as np
import pandas as pd
import os.path
import sqlite3
import time
from typing import Final

# == FUNC ==
def calc_volatility(data = []) -> int:

    """
    
    Calculate volatility
    
    Calculate volatility as the standard deviation of time series data.
    
    Parameters
    ----------
    data : list, required
        A list with time series data. This means that the data provided
        must be ordered in ascending sequence.
   
    Returns
    -------
    int
        The standard deviation on differences to the previous day in
        percent.
        
    Notes
    -----
    Module is based on:
    * Stiftung Warentest: ABC für Anleger - Volatilität
      https://www.test.de/ABC-fuer-Anleger-Volatilitaet-1049493-0/
    * GeVestor - Gerginov: Volatilität berechnen: Beispiel
      https://www.gevestor.de/finanzwissen/boerse/anlagenanalyse/volatilitat-berechnen-beispiel-655124.html
    
    """
    
    # == Begin ==
    # -- In --
    # Paraphrase.
    if __debug__:
        print(f"data:       {data}")
    # Check data.
    if not isinstance(data, list):
        raise ValueError("Wrong argument type for 'data'. Must be of list type!")
    elif len(data) == 0:
        raise ValueError("Wrong values in argument 'data'. List must contain any values!")   

    # -- Proc --
    df = pd.Series(data = data)
    df = df.pct_change()*100
    volatility = df.std()

    # -- Out --
    return volatility
    # == End ==
    
if __name__ == "__main__":

    # == Const ==
    DB_FILE: Final = "/home/gmaubach/Programming/StockTradingApp2/Data/StockTradingDB.sqlite"
    OUT_DIR: Final = os.path.dirname(DB_FILE)
    OUT_FILE: Final = "volatility.csv"
    START_DATE: Final = "2020-01-01"
    END_DATE: Final = "2020-12-31"
    
    # == VAR ==
    if __debug__:
        symbols = ["ATVI"]
        print(f"Symbols: {symbols}")
    else:
        cmd = f"SELECT DISTINCT symbol from ts_daily;"
        with sqlite3.connect(DB_FILE) as connection:
            cursor = connection.cursor()
            cursor.execute(cmd)
            connection.commit()
            symbols = [row[0] for row in cursor.fetchall()]
        connection.close()
        print(f"Symbols: {symbols}")
    
    # == Begin ==
    # -- In --
    volatility = {"symbol": [], "volatility": []}
    if __debug__:
        print(f"Volatility init: {volatility}")
    
    for symbol in symbols:
        print(f"Symbol: {symbol}")

        cmd = [
            f"SELECT close FROM ts_daily ",
            f"WHERE symbol = '{symbol}' ",
            f"AND (timestamp BETWEEN '{START_DATE}' AND '{END_DATE}') ",
            f"ORDER BY timestamp ASC;"
            ]
        cmd = "".join(cmd)
        if __debug__:
            print(f"SQL: {cmd}")
        
        with sqlite3.connect(DB_FILE) as connection:
             cursor = connection.cursor()
             cursor.execute(cmd)
             connection.commit()
             time_series = [row[0] for row in cursor.fetchall()]
        print(f"Time Series: {time_series}")
        connection.close()

        # -- PROC --
        volatility["symbol"].append(symbol)
        try:
            volatility["volatility"].append(calc_volatility(data = time_series))
        except:
            volatility["volatility"].append(np.nan)
        if __debug__:
            print(f"Volatility: {volatility}")
            time.sleep(3)

    df = pd.DataFrame(data = volatility)
    df.sort_values("volatility", ascending = False, inplace = True)
    print(f"Volatility Dataframe Head:\n {df.head()}")
    print(f"Volatility Dataframe Tail:\n {df.tail()}")

    # -- Out --
    # Flatfile.
    df.to_csv(os.path.join(OUT_DIR, OUT_FILE), index = False)
    
    # Database.
    with sqlite3.connect(DB_FILE) as connection:
        result = df.to_sql("t_volatility", connection, if_exists = "replace", index = False)
    connection.close()
    # == End ==

# EOF .

