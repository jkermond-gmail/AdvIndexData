USE [AdvIndexData]
GO

/****** Object:  Table [dbo].[JobIndexIds]    Script Date: 8/4/2020 9:31:22 AM ******/
/*
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[JobIndexIds_temp](
	[ClientID] [varchar](30) NOT NULL,
	[JobName] [varchar](80) NOT NULL,
	[Vendor] [varchar](50) NOT NULL,
	[IndexName] [varchar](20) NOT NULL,
	[AddDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[RunSecMasterList] [varchar](3) NOT NULL,
	[RunSecMasterListDate] [datetime] NOT NULL,
	[HistoryBeginDate] [datetime] NOT NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

*/

INSERT INTO JobIndexIds_temp
SELECT * FROM JobIndexIds
WHERE vendor = 'Russell' and ClientID = 'SystemClient'; 


SELECT * FROM JobIndexIds_temp

update JobIndexIds_temp
set JobName = 'Russell ICB Sector'
where JobName = 'Russell RGS Sector'

update JobIndexIds_temp
set vendor = 'RussellICB'

INSERT INTO JobIndexIds
SELECT * FROM JobIndexIds_temp
