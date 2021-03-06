
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
  set [SettingValue] = '01/02/2018'
  where [SettingName] = 'VIFLastProcessDate'

  update [IndexData].[dbo].[SystemSettings]
  set [SettingValue] = '12/31/2017'
  where [SettingName] = 'IndexDataProcessDate'

  update  [IndexData].[dbo].Jobs set [LastProcessDate] = '12/31/2017'
  WHERE  JobType = 'Vendor' and Active = 'Yes'

  