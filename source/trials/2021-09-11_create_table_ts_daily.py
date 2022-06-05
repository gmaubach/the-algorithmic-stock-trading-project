import sqlite3
from typing import Final

def create_table_ts_daily(db_file):

    r"""
    
    Create table "ts_daily"
    
    Create a table to store time series data.
    
    Parameters
    ----------
    db_file : string
        Path and filename of the database (required).
    
    """
       
    cmd = [
    "CREATE TABLE IF NOT EXISTS ts_daily ",
    "( ",    
    "symbol CHAR(12) NOT NULL, " 
    "timestamp DATE, " 
    "open DECIMAL(12,2), " 
    "high DECIMAL(12,2), " 
    "low DECIMAL(12,2), " 
    "close DECIMAL(12,2), " 
    "volume FLOAT " 
    "); "
    ]
    cmd = "".join(cmd)
    print(cmd)

    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        cursor.execute(cmd)

    connection.commit()
    connection.close()

if __name__ == "__main__":

    DB_FILE: Final = "/home/gmaubach/Programming/StockTradingApp2/Data/StockTradingDB.sqlite"
 
    create_table_ts_daily(DB_FILE)
 
# EOF .

