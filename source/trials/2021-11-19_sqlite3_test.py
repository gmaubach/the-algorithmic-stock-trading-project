import sqlite3
from sqlite3 import Error

con = sqlite3.connect("../Data/StockTradingDB2.sqlite3")
cur = con.cursor()
cur.execute("""
    CREATE TABLE t_company 
    (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(25) NOT NULL,
      ticker VARCHAR(4)
    );
    """)
    
cur.execute("""
    INSERT INTO t_company (name, ticker) VALUES ("MyCompany", "ABCD");
    """)

con.commit()

