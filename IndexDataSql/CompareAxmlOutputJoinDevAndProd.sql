/****** Script for SelectTopNRows command from SSMS  ******/
use IndexData

SELECT /*dev.[Source] as dsource, prod.[Source] as psource, */
      dev.IndexName as dIndexName /*, prod.IndexName as pIndexName */
      ,dev.ReturnDate as dReturnDate /*, prod.ReturnDate as pReturnDate */
      ,dev.OutputType as dOutputType /*, prod.OutputType as pOutputType */
      ,dev.Identifier as dIdentifier /*, prod.Identifier as pIdentifier */
      ,dev.[Weight] as dWeight, prod.[Weight] as pWeight
      ,dev.Irr as dIrr, prod.Irr as pIrr
  FROM AxmlOutput dev
  inner join AxmlOutput prod on 
	prod.Identifier = dev.Identifier and
	prod.IndexName = dev.IndexName and
	prod.ReturnDate = dev.ReturnDate and 
	prod.OutputType = dev.OutputType
  where dev.IndexName = 'r3000' and dev.ReturnDate = '12/31/2018' and dev.OutputType = 'Constituent' and dev.Source = 'Dev' and
  prod.IndexName = 'r3000' and prod.ReturnDate = '12/31/2018' and prod.OutputType = 'Constituent' and prod.Source = 'Prod'