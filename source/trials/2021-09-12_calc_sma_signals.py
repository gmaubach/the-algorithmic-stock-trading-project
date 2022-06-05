#!/usr/bin/env python
# -*- coding: utf-8 -*-
#--------1---------2---------3---------4---------5---------6---------7----|

# == INCLUDE ==
from datetime import datetime
import math
import matplotlib.pyplot as plt
import numpy as np
import os.path
import pandas as pd
import sqlite3
import sys
from termcolor import colored as cl 
import time
from typing import Final

path_2_my_libs: Final = "~/Programming/StockTradingApp2/Source/"

# == CONFIG ==
sys.path.append(path_2_my_libs)

# == VAR ==

# == FUNC ==
def calc_simple_mov_avg(data = [], period = 1) -> pandas.DataFrame:

    r"""
    
    Calculate Simple Moving Average
    
    Parameters
    ----------
    
    data : list, required
        List of time series data.
    period : integer, required
        Number representing the period to include into moving average.
        
    Returns
    -------
    pandas.DataFrame
        The simple moving average for the provided list data.
    
    Notes
    -----
    Module based on
    Adithyan, Nikhil: Algorithmic Trading with SMA in Python,
    Creating and backtesting an SMA trading strategy in python,
    https://medium.com/codex/algorithmic-trading-with-sma-in-python-7d66008d37b1,
    last access So 2021-09-12
    
    Modularisation and preparation for optimization by the author.
    """

    # == BEGIN ==    
    # -- IN --
    # Paraphrase.
    if __debug__:
        print(f"data: {data}")
        print(f"period: {period}")
    
    # Check data.
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"Wrong values for argument 'data'!")
    
    # -- PROC --
    sma = data.rolling(window = period).mean()
    
    # -- OUT --
    return pd.DataFrame(sma)
    # == End ==
    
def plot_simple_mov_avg(
    symbol: str = "",
    data: pandas.DataFrame,
    sma_short: int,
    sma_long: int,
    show_plot: bool = False,
    filename: str = ""):
    
    r"""
    
    Plot Simple Moving Average Values
    
    Parameters
    ----------
    symbol : str, required
        The stock identifier given as a symbol.
    data : pandas.DataFrame
        The dataframe must contain the variables
        'close': closing stock rates
        'sma_short': short moving average values
        'sma_long': long moving average values
    sma_short: int, required
       Amount of steps for short period of simple moving average
    sma_long: int, required
       Amount of steps for long period of simple moving average
    show_plot: bool
        Flag to show the plot during interactive execution.
    filename: str
       Path and filename for plot to be stored. If empty plot is
       not saved to disk.
   
    """
    
    # == BEGIN ==
    # -- In --
    # Paraphrase.
    if __debug__:
        print(f"'symbol': {symbol}")
        print(f"'data': {data.head()}")
        print(f"'sma_short': {sma_short}")
        print(f"'sma_long': {sma_long}")
        print(f"'show_plot': {show_plot}")
        print(f"'filename': {filename}")
        
    # Check Symbol.
    if not isinstance(symbol, str) and len(symbol) == 0:
        raise ValueError(f"Wrong argument value for {symbol}. Must be string in upper case.")
    
    # -- CONFIG --
    plt.style.use('fivethirtyeight')
    plt.rcParams['figure.figsize'] = (15, 8)
    plt.plot(data['close'], label = symbol, linewidth = 5, alpha = 0.3)
    plt.plot(data['sma_short'], label = 'SMA SHORT')
    plt.plot(data['sma_long'], label = 'SMA_LONG')
    plt.title(f'{symbol}: Simple Moving Averages ({sma_short}, {sma_long})')
    plt.legend(loc = 'upper left')
    
    # -- OUT --
    if show_plot:
        plt.show()

    if len(filename) > 0:
       plt.savefig(fname = filename, orientation = "landscape")	
    # == End ==

if __name__ == "__main__":
    
    # == CONST ==
   
    # == VAR ==
    periods = [["short", 20], ["long", 50]]
     
    # == BEGIN ==
    # -- PROC --
    # Calculate Simple Moving Average.
    for period in periods:
        df_ts[f'sma_{period[0]}'] = calc_simple_mov_avg(df_ts['close'], period[1])
    # == END ==
        
# EOF .    

