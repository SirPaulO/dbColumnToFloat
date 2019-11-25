#!/usr/bin/python
# -*- coding: latin-1 -*-

# Imports
import re
from msSQLBridge import *


class dbColumnToFloat(object):
  """ Converts SQL columns from varchar to float. Trying to parse as much as possible to floats the originals values """


  def __init__(self, DB_HOST, DB_USER, DB_PASS, DB_DATABASE, filterFn = None):
    """ Shold provide the database connection info """
    # Counters
    self.cnt_total    = 0
    self.cnt_modified = 0
    self.cnt_error    = 0

    self.filterFn     = filterFn

    self.queries      = None
    self._DEBUG       = 0
    self.modifiedRows = {}
    '''
    modifiedRows = {
      "String: Table name" : {
        "tblIDName": "ID Column name",
        "rowid": {
          "rowid" : rowid,
          "columns":[String, String, String, ...],
          "values":[Float, Float, Float, ...],
        },
      },
      ...
    }
    '''

    self._dbConnect(DB_HOST, DB_USER, DB_PASS, DB_DATABASE)


  def setDebug(self, level):
    """ Set the DEBUG level to print information.

    Levels:
    - DEBUG_NONE: Not debug info will be printed
    - DEBUG_RELEVANT: Prints relevant information about the rows that couldn't been convered to float
    - DEBUG_ERRORS: Prints DEBUG_RELEVANT + the Exception messages
    - DEBUG_ALL: Prints DEBUG_ERRORS + information about the proccess
    """
    self._DEBUG = 1 if level == "DEBUG_RELEVANT" else 2 if level == "DEBUG_ERRORS" else 3 if level == "DEBUG_ALL" else 0


  def setQueries(self, queries):
    """ Set the List of Dicts wich contains tables and columns to convert.

    List of Dicts format:
    queries = [
      {
        "table"    : "Table name",
        "columns" : ["Column 1", "Column 2"],
        "id"       : "Column ID name",
        "where"    : "" # Custom where to filter columns. Probably it's a good idea to left out the 0.0 values
      },
      ...
    ]
    """
    self.queries = queries


  def run(self):
    """ Starts the convertions! """
    self._convertColumnsToFloat()


  def _dbConnect(self, DB_HOST, DB_USER, DB_PASS, DB_DATABASE):
    """ Create the DB connection """
    self._db = msSQLBridge(DB_HOST, DB_USER, DB_PASS, DB_DATABASE)


  def _convertColumnsToFloat(self):
    if self.queries is None:
       raise Exception("You must set the queries before run!")

    for query in self.queries:
      table = query["table"]
      tblIDName = query["id"]

      # First query will try to "clean" some basic db
      for column in query['columns']:
        querystr = "UPDATE {0} SET {1} = '0.0' WHERE {1} = '' OR {1} = '0'".format(table, column)
        self._db.exec(querystr)

      # Fetch rows
      querystr = "SELECT {0}, {1} FROM {2}".format(tblIDName, ", ".join(query["columns"]), table)

      if query["where"] != "":
        querystr = querystr + " WHERE " + query["where"]

      results = self._db.exec(querystr)

      if(self._DEBUG > 2):
        print("Fetch: {0} rows from table: {1}".format(len(results), table))

      for result in results:
        rowid = result[0] # Left the table id out

        for index, n in enumerate(result[1:]):
          try:
            self.cnt_total += 1
            column = query["columns"][index] # Column name

            floating = self._stringToFloat(str(n)) # Modified Value

            if str(n) != str(floating) and str(n) != "0" and str(n) != "0.0" and str(n) != "":
              self._appendToModified(rowid, table, column, tblIDName, floating)
              if(self._DEBUG > 2):
                print("Tbl: {0} | id: {1} | clm: {2} | orig: {3} | repl: {4}".format(table, rowid, column, n, floating))

          except Exception as e:
            self.cnt_error += 1
            self._appendToModified(rowid, table, column, tblIDName, 0.0)
            if(self._DEBUG > 0):
              print("\n==")
              print("Tbl: {0} | id: {1} | clm: {2} | orig: {3} | repl: 0.0".format(table, rowid, column, n))
              if(self._DEBUG > 1):
                print(e)

    self._updateModified()


  def _stringToFloat(self, value):
    n = self._clearString(value)[::-1] # Limpiar e invertir

    hasDot = n.find(".") > -1
    dotPosition = n.find(".")

    hasSemicolon = n.find(",") > -1
    semicolonPosition = n.find(",")

    decimal    = None
    centesimal = None

    if hasDot and hasSemicolon: # 1.123.456,00 o 1,123,456.00
      if dotPosition < semicolonPosition: # 1,123,456.00
        decimal = "."
        centesimal = ","
      else: # 1.123.456,00
        decimal = ","
        centesimal = "."
    elif hasDot: # 1132456.00 o 1.123.456
      if dotPosition <= 2: # 1132456.00 o 1.123.456.00
        n = n.replace(".", "")
        n = n[:dotPosition] + "." + str(n[dotPosition:])
        decimal = "."
        centesimal = ","
      elif dotPosition > 3: # 176495.97999999998 (REALLY BAD FORMAT)
        n = str(n[dotPosition-2:dotPosition]) + "." + n[dotPosition+1:]
        decimal = "."
        centesimal = ","
      else: # 1.123.456
        decimal = ","
        centesimal = "."
    elif hasSemicolon: #1132456,00 o 1,123,456
      if semicolonPosition <= 2: # 1132456,00 o 1,123,456,00 (BAD FORMAT)
        n = n.replace(",", "")
        n = n[:semicolonPosition] + "," + str(n[semicolonPosition:])
        decimal = ","
        centesimal = "."
      elif semicolonPosition > 3: # 176495,97999999998 (REALLY BAD FORMAT)
        n = str(n[semicolonPosition-2:semicolonPosition]) + "," + n[semicolonPosition+1:]
        decimal = ","
        centesimal = "."
      else: # 1,123,456
        decimal = "."
        centesimal = ","

    n = n.replace(centesimal, "") if centesimal is not None else n
    n = n.replace(decimal, ".")   if decimal    is not None else n
    n = n[::-1]

    n = float(n)

    return n


  def _clearString(self, n):
    if n == "" or n == "0" or n == "0.0" or len(n) < 1 or n == None:
      n = "0.0"
      return n

    if self.filterFn != None:
      n = self.filterFn(n)
    else:
      n = n.lower()
      n = n.replace(";", ".")
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


  def _appendToModified(self, rowid, table, column, tblIDName, value):
    self.cnt_modified += 1
    rowid = str(rowid)
    if table in self.modifiedRows:
      if rowid in self.modifiedRows[table]:
        self.modifiedRows[table][rowid]["columns"].append(column)
        self.modifiedRows[table][rowid]["values"].append(value)
      else:
        self.modifiedRows[table][rowid] = {"columns": [column], "values": [value]}
    else:
      self.modifiedRows[table] = {rowid: {"columns": [column], "values": [value]}, "tblIDName": tblIDName}


  def _updateModified(self):
    if(self._DEBUG > 2):
      print("\n ** UPDATING MODIFIED")

    queries = []

    for table in self.modifiedRows:
      for rowid in self.modifiedRows[table].keys():
        if rowid == "tblIDName" :continue # Skip this key

        vals = ", ".join( [ column +" = '" + str( self.modifiedRows[table][rowid]["values"][self.modifiedRows[table][rowid]["columns"].index(column)] ) +"' " for column in self.modifiedRows[table][rowid]["columns"] ] )
        query = "UPDATE {0} SET {1} WHERE {2} = {3}".format(table, vals, self.modifiedRows[table]["tblIDName"], rowid)
        queries.append(query)

    for query in queries:
      try:
        self._db.exec(query)
      except Exception as e:
        if(self._DEBUG > 1):
          print(e)

    self._db.commit()

    if(self._DEBUG > 0):
      print("/** Totals **/")
      print("Total: " + str(self.cnt_total))
      print("Modified: " + str(self.cnt_modified))
      print("Errors: " + str(self.cnt_error))


  def changeColumnTypeToFloat(self):
    for query in self.queries:
      for column in query["columns"]:
        try:
          sql = "sp_ConvertColumnToFloat '{0}', '{1}'".format(query["table"], column)
          self._db.execCommit(sql)
        except Exception as e:
          if(self._DEBUG > 0):
            print(sql)
            if(self._DEBUG > 1):
              print(e)

