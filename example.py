#!/usr/bin/python
# -*- coding: latin-1 -*-

from dbColumnToFloat import *

DB_HOST          = "127.0.0.1"
DB_USER          = "username"
DB_PASS          = "password"
DB_DATABASE      = "DatabaseName"

queries = [
  {
    "table"    : "tableName",
    "columns" : ["Column1", "Column2"],
    "id"       : "id",
    "where"    : " WHERE Column1 != '0.0', Column2 != '0.0' "
  }
]

# Information in columns like..
#data = [" ","12313.6546465465", "12313,6546465465" ,"123, 321.0058","123..321....132,,0058","123.321.132,0058","123,321,132.0058","123.321,0058","123,321.0058","123321.0058","123321,0058"]

# Optional custom cleaning function
def customFilterFn(n):
  """ This function cleans the string to try cast as Float. Should return a clear String containing the float """
  n = n.lower()
  n = n.replace(";", ".")
  n = n.replace("mil", "000")
  n = n.replace("$", "")
  n = n.replace("_", "")
  n = n.replace("-", "")
  n = n.replace(" ", "")
  n = n.replace("\t", "")

  n = re.sub('[A-z]','', n)
  n = re.sub('(\\.)+', '.', n)
  n = re.sub('(\\,)+', ',', n)

  if n[-1] == "." or n[-1] == ",":
    n = n[:-1]

  if n == "" or n == "." or n == ",":
    n = "0.0"

  return n

# customFilterFn is an optional parameter
converter = dbColumnToFloat(DB_HOST, DB_USER, DB_PASS, DB_DATABASE, customFilterFn)

converter.setQueries(queries)

converter.setDebug("DEBUG_ALL")

converter.run()

# This is optional. You can change the column type in other custom way.
# This function calls the Stored Procedure to change the column type
converter.changeColumnTypeToFloat()

