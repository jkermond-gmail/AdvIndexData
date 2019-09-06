USE IndexData
GO

select id from HistoricalSecurityMasterFull where id not in 
(select id from HistoricalSecurityMasterFullCopy)

INSERT INTO HistoricalSecurityMasterFullChanges
(id, ProcessDate, ChangeType, CusipNew, TickerNew, CompanyNameNew, SectorCodeNew)
select id, BeginDate, 'Add', Cusip, Ticker, CompanyName, SectorCode from HistoricalSecurityMasterFull where id = '105191'

 delete from HistoricalSecurityMasterFullChanges 

select * from HistoricalSecurityMasterFullChanges 


INSERT INTO HistoricalSecurityMasterFullChanges
           (id, ProcessDate, ChangeType, CusipNew, TickerNew, CompanyNameNew, SectorCodeNew)
           select id, BeginDate, 'Add', Cusip, Ticker, CompanyName, SectorCode from HistoricalSecurityMasterFull where id = '100089'

select h.id, h.Ticker, h.Cusip, h.CompanyName, h.SectorCode, hprev.Ticker as TickerOld, hprev.Cusip as CusipOld, hprev.CompanyName as CompanyNameOld, hprev.SectorCode as SectorCodeOld from HistoricalSecurityMasterFull h
inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
where h.EndDate = '09/04/2019' and hprev.EndDate = '09/03/2019'
and h.Vendor = 'R' and hprev.Vendor = 'R' and (h.Ticker<> hprev.Ticker OR h.Cusip<> hprev.Cusip or h.CompanyName<> hprev.CompanyName or h.SectorCode<> hprev.SectorCode)


select h.Ticker, h.Cusip, h.CompanyName, h.SectorCode, hprev.Ticker as TickerOld, 
       hprev.Cusip as CusipOld, hprev.CompanyName as CompanyNameOld, hprev.SectorCode as SectorCodeOld 
	   from HistoricalSecurityMasterFull h
inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
where h.id = @id and hprev.id = @id



select h.id from HistoricalSecurityMasterFull h
inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
where h.EndDate = '09/04/2019' and hprev.EndDate = '09/03/2019'
and h.Vendor = 'R' and hprev.Vendor = 'R' 
and (h.Ticker<> hprev.Ticker OR h.Cusip<> hprev.Cusip or h.CompanyName<> hprev.CompanyName or h.SectorCode<> hprev.SectorCode)



GO

INSERT INTO table2 (column1, column2, column3, ...)
SELECT column1, column2, column3, ...
FROM table1
WHERE condition; 


