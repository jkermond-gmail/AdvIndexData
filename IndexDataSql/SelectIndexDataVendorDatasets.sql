/****** Script for SelectTopNRows command from SSMS  ******/
Use IndexData
SELECT distinct Vendor, Dataset
FROM Jobs
where JobType = 'Vendor' and Active = 'Yes'
order by Vendor, DataSet

 
