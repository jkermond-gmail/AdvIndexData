/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [id]
      ,[Ticker]
      ,[Cusip]
      ,[Description]
      ,[BeginningDate]
      ,[EndDate]
      ,[sourcefile]
      ,[dateModified]
  FROM [IndexData].[dbo].[HistoricalSecurityMaster]
  where ticker in  ('adt','altr','btu','cb_old','dxc','fti','igt','life','wrk') 
  or ticker like 'adt.%'
  or ticker like 'altr.%'
  or ticker like 'btu.%'
  or ticker like 'cb.%'
  or ticker like 'dxc.%'
  or ticker like 'fti.%'
  or ticker like 'igt.%'
  or ticker like 'life.%'
  or ticker like 'wrk.%'

  SELECT *
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull]
  where ticker in  ('adt','altr','btu','cb_old','dxc','fti','igt','life','wrk') 
  or ticker like 'adt.%'
  or ticker like 'altr.%'
  or ticker like 'btu.%'
  or ticker like 'cb.%'
  or ticker like 'dxc.%'
  or ticker like 'fti.%'
  or ticker like 'igt.%'
  or ticker like 'life.%'
  or ticker like 'wrk.%'