#!/usr/bin/env python
# -*- coding: utf-8 -*-
#--------1---------2---------3---------4---------5---------6---------7----|

import sqlite3

def read_symbols_from_company_list(db_file):

    r"""
    
    Read symbols from initial company list
    :param string db_file: the database path and filename
    :return: symbols
    :rtype: list
	  :Example: read_symbols_from_company_list(
	            ".../Data/StockTradingDB.sqlite")
	  
    """
    
    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT Symbol FROM company_list;")
        list_of_rows = cursor.fetchall()
        symbols = [row[0] for row in list_of_rows]
        return(symbols)

# EOF .

