/****** Script for SelectTopNRows command from SSMS  ******/
--SELECT TOP 1000 [SettingName]
--      ,[SettingValue]
--  FROM [AmdVifsDB].[dbo].[SystemSettings]

  update [AmdVifsDB].[dbo].[SystemSettings]
  set [SettingValue] = '01/02/2018'
  where [SettingName] = 'VIFLastProcessDate'

  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '12/29/2017'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '12/29/2019'
  WHERE  JobType = 'Vendor' and Active = 'Yes'
