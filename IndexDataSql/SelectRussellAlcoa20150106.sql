/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [FileDate]
      ,[CUSIP]
      ,[Ticker]
      ,[MktValue]
      ,[SharesDenominator]
      ,[Sector]
      ,[SecurityReturn]
  FROM [IndexData].[dbo].[RussellDailyHoldings1]
  where FileDate = '01/06/2014' and cusip in ('01381710')
  order by ticker

SELECT TOP 1000 [id]
      ,[Ticker]
      ,[Cusip]
      ,[Description]
      ,[BeginningDate]
      ,[EndDate]
      ,[sourcefile]
      ,[dateModified]
  FROM [IndexData].[dbo].[HistoricalSecurityMaster]
  where cusip in ('01381710')

  SELECT TOP 1000 *
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull]
  where cusip in ('01381710')

  SELECT TOP 1000 *
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull]
  where StockKey in ('5076')

  SELECT TOP 1000 *
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull]
  where ticker in ('aa')

  SELECT *
  FROM [IndexData].[dbo].[HistoricalSecurityMaster]
  where ticker in ('arnc')

  SELECT TOP 1000 [Vendor]
      ,[ChangeDate]
      ,[OldSymbol]
      ,[NewSymbol]
      ,[CompanyName]
  FROM [IndexData].[dbo].[HistoricalSymbolChanges]
  where OldSymbol = '01381710'

  SELECT TOP 1000 [Vendor]
      ,[ChangeDate]
      ,[OldSymbol]
      ,[NewSymbol]
      ,[CompanyName]
  FROM [IndexData].[dbo].[HistoricalSymbolChanges]
  where OldSymbol = '01381750'

  SELECT TOP 1000 [Vendor]
      ,[ChangeDate]
      ,[OldSymbol]
      ,[NewSymbol]
      ,[CompanyName]
  FROM [IndexData].[dbo].[HistoricalSymbolChanges]
  where OldSymbol = '03965L10'