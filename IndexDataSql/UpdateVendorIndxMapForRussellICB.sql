
  USE [AdvIndexData]
GO

/****** Object:  Table [dbo].[VendorIndexMap]    Script Date: 8/4/2020 11:43:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[VendorIndexMap_temp](
	[Vendor] [varchar](50) NOT NULL,
	[Dataset] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_Dataset_temp]  DEFAULT (''),
	[IndexName] [varchar](20) NOT NULL,
	[IndexClientName] [varchar](20) NOT NULL,
	[IndexSpec] [char](1) NOT NULL,
	[Supported] [varchar](3) NOT NULL,
	[AxmlSectorFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlSectorFile_temp]  DEFAULT (''),
	[AxmlConstituentFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlConstituentFile_temp]  DEFAULT (''),
	[AxmlBatchID] [int] NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlBatchID_temp]  DEFAULT ((0))
) ON [PRIMARY]

GO
use AdvIndexData
SET ANSI_PADDING OFF
GO

INSERT INTO VendorIndexMap_temp
SELECT * FROM VendorIndexMap
WHERE vendor = 'Russell'


SELECT * FROM VendorIndexMap_temp

SELECT * FROM VendorIndexMap


update VendorIndexMap_temp
set Vendor = 'RussellICB'



INSERT INTO VendorIndexMap
SELECT * FROM VendorIndexMap_temp


