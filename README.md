# Change Table Column to Float in Microsoft SQL Server
This is a Python 3 script that converts database columns from varchar to float. Triying agressively to cast the column data.
Originally intended for MS SQL Server, but it can work with any database, you will have to change the column type manually, or adapt the Stored Procedure provided here.

## Dependencies
  - pyodbc
  - msSQLBridge (pyodbc bridge class provided here)

## Installation
  - Install pyodbc ($ pip install pyodbc)
  - Clone the repo

## Usage
  - First insert the Stored Procedure into database
  - Check/modify example file provided here
  - Run the script
  - Done!


### Thanks:
  - StackOverflow ;)
