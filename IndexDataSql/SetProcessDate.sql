/****** Script for SelectTopNRows command from SSMS  ******/
--SELECT TOP 1000 [SettingName]
--      ,[SettingValue]
--  FROM [AmdVifsDB].[dbo].[SystemSettings]

--- THis is when S&P moved to a new format
  --update [AmdVifsDB].[dbo].[SystemSettings]
  --set [SettingValue] = '04/01/2015'
  --where [SettingName] = 'VIFLastProcessDate'

  --update [IndexData].[dbo].[SystemSettings]
  --set [SettingValue] = '03/31/2015'
  --where [SettingName] = 'IndexDataProcessDate'

  --update  [IndexData].[dbo].Jobs set [LastProcessDate] = '03/31/2015'
  --WHERE  JobType = 'Vendor' and Active = 'Yes'

  update [AmdVifsDB].[dbo].[SystemSettings]
  set [SettingValue] = '12/03/2018'
  where [SettingName] = 'VIFLastProcessDate'

  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '11/30/2018'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '12/31/2017'
  WHERE  JobType = 'Vendor' and Active = 'Yes'