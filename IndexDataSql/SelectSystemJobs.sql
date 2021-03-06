/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [ClientID]
      ,[JobName]
      ,[JobType]
      ,[Schedule]
      ,[LastProcessDate]
      ,[LastFilterFromDate]
      ,[LastFilterToDate]
      ,[Active]
      ,[Vendor]
      ,[InputFormat]
      ,[RefReport]
      ,[DataSet]
      ,[WorkflowStatus]
      ,[WorkflowStatusDatetime]
      ,[AddDate]
      ,[ModifiedDate]
  FROM [IndexData].[dbo].[Jobs]

use indexdata
SELECT * FROM Jobs
WHERE  Vendor = 'Russell' and DataSet = 'RGS' and JobType = 'Vendor' and Active = 'Yes'
order by InputFormat

SELECT * FROM Jobs
WHERE  Vendor = 'standardandpoors' and JobType = 'Vendor' and Active = 'Yes'
order by InputFormat
 