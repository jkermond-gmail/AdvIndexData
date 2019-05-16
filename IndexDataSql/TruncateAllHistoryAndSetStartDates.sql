use IndexData

  truncate table historicalsecuritymasterfull
  truncate table historicalsymbolchanges
  truncate table RussellDailyHoldings1
  truncate table RussellDailyHoldings2
  truncate table RussellDailyIndexReturns
  truncate table RussellDailyTotals
  truncate table SnpDailyClosingHoldings
  truncate table SnpDailyIndexReturns
  truncate table SnpDailyOpeningHoldings
  truncate table TotalReturns
  truncate table ProcessStatus

  /*
  update [AmdVifsDB].[dbo].[SystemSettings]
  set [SettingValue] = '01/04/2016'
  where [SettingName] = 'VIFLastProcessDate'

  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '12/31/2015'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '12/31/2015'
  WHERE  JobType = 'Vendor' and Active = 'Yes'

  */

  update [AmdVifsDB].[dbo].[SystemSettings]
  set [SettingValue] = '01/02/2014'
  where [SettingName] = 'VIFLastProcessDate'

  update [AmdVifsDB].[dbo].[Vifs]
  set [LastProcessDate] = '01/02/2014'
  
  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '12/31/2013'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '12/31/2013'
  WHERE  JobType = 'Vendor' and Active = 'Yes'

delete   FROM [IndexData].[dbo].[ProcessStatus]
  where ProcessDate >= '12/31/2013'
