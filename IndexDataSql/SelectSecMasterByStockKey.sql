use IndexData

select * from [IndexData].[dbo].[HistoricalSecurityMasterFull] 
where StockKey in (select StockKey from (
SELECT [StockKey], count(*) as StockKeyCount
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull] 
  group by [StockKey]
  having count(*) > 2)as tmp)
  order by BeginDate

SELECT [StockKey], count(*) as StockKeyCount
  FROM [IndexData].[dbo].[HistoricalSecurityMasterFull] 
  group by [StockKey]
  having count(*) > 2
  order by StockKey

select * from  HistoricalSecurityMasterFull
where StockKey in
(select StockKey from HistoricalSecurityMasterFull
where ticker in ('aa', 'arcn'))

select * from  HistoricalSecurityMasterFull
where StockKey in
(select StockKey from HistoricalSecurityMasterFull
where ticker in ('ace'))

select * from  HistoricalSecurityMasterFull
where StockKey in
(select StockKey from HistoricalSecurityMasterFull
where ticker in ('adt'))

select top 1 EndDate from HistoricalSecurityMasterFull
order by enddate desc

select * from HistoricalSecurityMasterFull where ticker like
('fti%')

select * from HistoricalSecurityMasterFull where ticker in
('csc', 'dxc') 
order by ticker

select * from HistoricalSecurityMasterFull where StockKey in
('34719') 
order by EndDate


select  ticker, StockKey, enddate from HistoricalSecurityMasterFull where enddate < 
(select top 1 EndDate from HistoricalSecurityMasterFull order by enddate desc)
order by ticker asc, enddate desc

select * from HistoricalSecurityMasterFull
order by ticker, enddate desc


select distinct ticker, StockKey from HistoricalSecurityMasterFull where enddate = 
(select top 1 EndDate from HistoricalSecurityMasterFull
order by enddate desc)

select ticker from HistoricalSecurityMasterFull where StockKey in
(select distinct StockKey from HistoricalSecurityMasterFull where enddate < 
(select top 1 EndDate from HistoricalSecurityMasterFull order by enddate desc)
and StockKey not in 
(select distinct StockKey from HistoricalSecurityMasterFull where enddate = 
(select top 1 EndDate from HistoricalSecurityMasterFull
order by enddate desc)))
and ticker in 
(select distinct ticker from HistoricalSecurityMasterFull where enddate = 
(select top 1 EndDate from HistoricalSecurityMasterFull
order by enddate desc))

select top 1 ticker from HistoricalSecurityMasterFull where StockKey in (
select StockKey from HistoricalSecurityMasterFull
where Cusip = '01381710')
order by EndDate desc

