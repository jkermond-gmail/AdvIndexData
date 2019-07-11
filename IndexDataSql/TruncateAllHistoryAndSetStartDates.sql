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
  

  update [AmdVifsDB].[dbo].[SystemSettings]
  set [SettingValue] = '06/26/2019'
  where [SettingName] = 'VIFLastProcessDate'

  update [AmdVifsDB].[dbo].[Vifs]
  set [LastProcessDate] = '06/26/2019'
  
  /* Set the values below 1 business day back */
  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '06/25/2019'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '06/25/2019'
  WHERE  JobType = 'Vendor' and Active = 'Yes'

delete   FROM [IndexData].[dbo].[ProcessStatus]
  where ProcessDate > '06/25/2019'
