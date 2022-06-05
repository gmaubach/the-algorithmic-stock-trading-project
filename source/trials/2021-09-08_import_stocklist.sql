# Import Company List
# Source:
#   https://www.sqlitetutorial.net/sqlite-commands/
#   https://www.sqlitetutorial.net/sqlite-import-csv/
#   https://stackoverflow.com/questions/21758769/running-a-sqlite3-script-from-command-line
#
# call with
# sqlite3 ../Data/StockTradingAppDB.sqlite < import_stocklist.sql

.mode csv
.import ../Data/2021-06-07-1742_Company-List-with-Symbols.csv company_list 

# EOF .

