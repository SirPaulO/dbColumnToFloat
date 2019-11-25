#!/usr/bin/python
# -*- coding: latin-1 -*-

# Imports
import pyodbc

class msSQLBridge(object):
  """Bridge for MS SQL Server"""
  def __init__(self, DB_HOST, DB_USER, DB_PASS, DB_DATABASE):
    # DB
    self.DB_HOST     = DB_HOST
    self.DB_USER     = DB_USER
    self.DB_PASS     = DB_PASS
    self.DB_DATABASE = DB_DATABASE
    self._CNXN       = None
    self._CNXNCursor = None


  def setCNXN(self, connectString = None):
    if connectString is not None:
      self._CNXN = pyodbc.connect(connectString)
    else:
      try:
        self._CNXN = pyodbc.connect("Driver={SQL Server};Server="+self.DB_HOST+";Database="+self.DB_DATABASE+";uid="+self.DB_USER+";pwd="+self.DB_PASS+";")
      except Exception as e:
        if "4060" in str(e):
          raise Exception("The login failed. (4060) (SQLDriverConnect)")
        else:
          raise e
    return self._CNXN


  def setCursor(self, cursor):
    self._CNXNCursor = cursor
    return self._CNXNCursor


  def getCursor(self):
    if self._CNXNCursor is None:
      return self.setCursor(self.getCNXN().cursor())
    return self._CNXNCursor


  def getCNXN(self):
    if self._CNXN is None:
      return self.setCNXN()
    return self._CNXN


  def exec(self, query):
    cursor = self.getCursor()
    cursor.execute(query)
    try:
      result = cursor.fetchall()
    except Exception as e:
      result = True
    return result


  def execCommit(self, query):
    cursor = self.getCursor()
    cursor.execute(query)
    try:
      result = cursor.fetchall()
    except Exception as e:
      result = True
    self.getCNXN().commit()
    return result


  def commit(self):
    cursor = self.getCursor()
    return self.getCNXN().commit()

