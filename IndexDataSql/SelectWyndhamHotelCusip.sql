/****** Script for SelectTopNRows command from SSMS  ******/
Use IndexData
SELECT TOP 1000 [ChangeDate]
      ,[OldSymbol]
      ,[NewSymbol]
      ,[CompanyName]
  FROM [IndexData].[dbo].[HistoricalSymbolChanges]
  where OldSymbol = '98310W10'