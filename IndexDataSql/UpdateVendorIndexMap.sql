/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [Vendor]
      ,[Dataset]
      ,[VendorIndexName]
      ,[AdventIndexName]
      ,[IndexSpec]
      ,[Supported]
  FROM [IndexData].[dbo].[VendorIndexMap]

  --use indexdata
  --update VendorIndexMap
  --set dataset = AdventIndexName where vendor = 'StandardAndPoors'

  --delete from VendorIndexMap
  --where Supported <> 'Yes'