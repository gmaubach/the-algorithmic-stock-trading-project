# execute_sql.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""
Execute any SQL command on a SQLite3 database

After the connection to the database is established each SQL statement
is executed one by one. Multiple statements in a single call to the
execute_sql() function can not be processed.

Available functions:
- open_connection: open a connection to a database using a file name
- execute_sql: execute a SQL statement
- close_connection: commit commands to the database and close connection

Sources:
* https://www.sqlitetutorial.net/sqlite-python/create-tables/
* https://www.tutorialspoint.com/sqlite/sqlite_python.htm
* https://www.sqlitetutorial.net/sqlite-python/insert/

"""

import os.path
import sqlite3

#--------1---------2---------3---------4---------5---------6---------7----|

def open_connection(file: str) -> sqlite3.Connection:
    u"""
    Open a connection to a SQLite3 database
    
    Args:
        file: database file
        
    Raises:
        Error if
    
    Returns:
        Connection object or None
    """
 
    con = None
    try:
        con = sqlite3.connect(file)
        return con
    except sqlite3.Error as e:
        print(e)
    return con
    
def execute_sql(con, statement):
    u"""
    Execute SQL statement on SQLite3 database
    
    Args:
        con: Connection object
        statement: SQL statement to execute
        
    Raises:
        Error is the statement could not be executed.
    
    Returns:
        nothing
    """
    #if __debug__:
        #print(con)
        #print(statement)
        
    try:
        c = con.cursor()
        c.execute(statement)
        con.commit()
    except:
        print("FAILED: ", statement)
        
def close_connection(con):
    u"""Commits commands and closes connection
    """
    con.commit()
    con.close()
 
if __name__ == '__main__':

    db_file = r"../Data/StockTradingDB2_Test.sqlite3"
    if not os.path.isfile(db_file):
        db_con = create_connection(db_file)
    
        with db_con:
            # create table t_company
            cmd = """
            CREATE TABLE IF NOT EXISTS t_company 
            (
              id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
              name VARCHAR(25) NOT NULL,
              ticker VARCHAR(4)
            );
            """     
            execute_sql(con = db_con, statement = cmd)
            
            # insert values into table t_company
            cmd = """
            INSERT INTO t_company (name, ticker) VALUES ("MyCompany", "ABCD");
            """
            execute_sql(con = db_con, statement = cmd)
                 
            # print values in table 't_company'
            cmd = """
            SELECT * FROM t_company LIMIT 10;
            """
            execute_sql(con = db_con, statement = cmd)
        
        close_connection(db_con)
    else:
        print("ERROR! Database already exists. Delete manually if needed!\n")
    
    print("Done!\n")

# EOF .
