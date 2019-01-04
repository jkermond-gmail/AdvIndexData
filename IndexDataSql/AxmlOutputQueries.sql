/****** Script for SelectTopNRows command from SSMS  ******/
SELECT 
      *
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev'

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev'


SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and VendorFormat = 'GICSSector'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and VendorFormat = 'GICSSector'
)

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and VendorFormat = 'GICSIndGrp'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and VendorFormat = 'GICSIndGrp'
)

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and VendorFormat = 'GICSIndustry'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and VendorFormat = 'GICSIndustry'
)

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and VendorFormat = 'GICSSubInd'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and VendorFormat = 'GICSSubInd'
)

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and VendorFormat = 'Security'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and VendorFormat = 'Security'
)

