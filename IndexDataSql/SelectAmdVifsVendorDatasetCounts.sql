/****** Script for SelectTopNRows command from SSMS  ******/
  use AmdVifsDB
  select count(*) as TheCount from VIFs
  where vendor = 'Russell' and DataSet = 'RGS' and [Application] = 'IDX' and Active = 'Yes' 