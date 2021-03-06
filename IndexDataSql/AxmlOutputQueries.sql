/****** Script for SelectTopNRows command from SSMS  ******/
delete FROM [IndexData].[dbo].[AxmlOutput] 




SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and OutputType = 'Security' and IndexName = 'r3000'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and OutputType = 'Security' and IndexName = 'r3000'
)

GO

SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Prod' and OutputType = 'Security' and IndexName = 'r3000'
and [Identifier] not in (
SELECT 
      [Identifier]
FROM [IndexData].[dbo].[AxmlOutput]
where [Source] = 'Dev' and OutputType = 'Security' and IndexName = 'r3000'
)

