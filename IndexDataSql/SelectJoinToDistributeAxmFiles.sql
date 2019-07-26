/****** Script for SelectTopNRows command from SSMS  ******/
use IndexData

SELECT j.ClientID, j.JobName, j.InputFormat, j.JobType, v.Dataset , v2.AdventIndexName, i.IndexName, v2.AxmlConstituentFile, v2.AxmlSectorFile   
  FROM VendorIndexMap v
  inner join Jobs j on j.DataSet = v.Dataset 
  inner join JobIndexIds i on i.JobName = j.JobName
  inner join VendorIndexMap v2 on v2.VendorIndexName = i.IndexName
  where v.AdventIndexName = 'r3000' and j.Active = 'Yes' and j.JobType = 'Client'
  order by ClientID