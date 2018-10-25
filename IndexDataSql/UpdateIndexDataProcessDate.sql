/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [SettingName]
      ,[SettingValue]
  FROM [IndexData].[dbo].[SystemSettings]

  GO

  use IndexData
  Update SystemSettings
  set SettingValue = '10/17/2018'
  where SettingName = 'IndexDataProcessDate'