use IndexData

DELETE FROM HistoricalSecurityMasterFullCopy

INSERT INTO HistoricalSecurityMasterFullCopy (
id, Ticker, Cusip, Vendor, StockKey, CompanyName, SectorCode, BeginDate, EndDate)
SELECT id, Ticker, Cusip, Vendor, StockKey, CompanyName, SectorCode, BeginDate, EndDate
FROM HistoricalSecurityMasterFull ORDER BY id

/* Get new adds */
select * from HistoricalSecurityMasterFull where id not in 
(select id from HistoricalSecurityMasterFullCopy)

/* Get deletes */
select * from HistoricalSecurityMasterFullCopy where EndDate = '09/03/2019' and id in
(select id from HistoricalSecurityMasterFull where EndDate = '09/03/2019')

/* Get updates */
select h.id, h.Ticker, h.Cusip, h.CompanyName, h.SectorCode, hprev.Ticker as TickerOld, hprev.Cusip as CusipOld, hprev.CompanyName as CompanyNameOld, hprev.SectorCode as SectorCodeOld from HistoricalSecurityMasterFull h 
inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
where h.EndDate = '09/04/2019' and hprev.EndDate = '09/03/2019'
and h.Vendor = 'R' and hprev.Vendor = 'R' and (h.Ticker <> hprev.Ticker OR h.Cusip <> hprev.Cusip or h.CompanyName <> hprev.CompanyName or h.SectorCode <> hprev.SectorCode)

select h.* from HistoricalSecurityMasterFull h 
where h.vendor = 'r'
order by h.EndDate desc

/*
Update HistoricalSecurityMasterFull
  set endDate = '09/04/2019' where enddate = '07/10/2019'

Update HistoricalSecurityMasterFullCopy
  set endDate = '09/03/2019' where enddate = '07/10/2019'
 */

select * from HistoricalSecurityMasterFull where vendor = 'r'

  Update HistoricalSecurityMasterFull
  set Vendor = 'R' where Vendor = 'S'