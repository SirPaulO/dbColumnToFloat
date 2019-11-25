SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:    SirPaulO
-- Create date: 2019-11-25
-- Description:
-- Converts given column to Float. With a flag to try convert the data or not.
-- Drops the Default value key if there is any
-- =============================================

CREATE PROCEDURE [dbo].[sp_ConvertColumnToFloat]
  -- Add the parameters for the stored procedure here
  @tblName AS VARCHAR(200),
  @clmName AS VARCHAR(200),
  @updateClm AS INT = 0
AS
BEGIN
  -- SET NOCOUNT ON added to prevent extra result sets from
  -- interfering with SELECT statements.
  SET NOCOUNT ON;

  DECLARE @table_id AS INT
  DECLARE @name_column_id AS INT
  DECLARE @sql nvarchar(255)

  -- Find table id
  SET @table_id = OBJECT_ID(@tblName)

  -- Find name column id
  SELECT @name_column_id = column_id
  FROM sys.columns
  WHERE object_id = @table_id
  AND name = @clmName

  -- Remove default constraint from name column
  SELECT @sql = 'ALTER TABLE '+@tblName+' DROP CONSTRAINT ' + D.name
  FROM sys.default_constraints AS D
  WHERE D.parent_object_id = @table_id
  AND D.parent_column_id = @name_column_id
  EXECUTE sp_executesql @sql

  IF @updateClm > 0
  BEGIN
    SET @sql = 'UPDATE '+@tblName+' SET '+@clmName+' = CONVERT(FLOAT, '+@clmName+')'
    EXEC(@sql)
  END

  SET @sql = 'ALTER TABLE '+@tblName+' ALTER COLUMN '+@clmName+' FLOAT'
  EXEC(@sql)

END
GO

