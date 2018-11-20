USE [master]
GO
/****** Object:  Database [IndexData]    Script Date: 11/20/2018 3:33:21 PM ******/
CREATE DATABASE [IndexData]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'IndexData', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData.mdf' , SIZE = 811584KB , MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
 LOG ON 
( NAME = N'IndexData_log', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData_log.ldf' , SIZE = 13217792KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO
ALTER DATABASE [IndexData] SET COMPATIBILITY_LEVEL = 120
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [IndexData].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [IndexData] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [IndexData] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [IndexData] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [IndexData] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [IndexData] SET ARITHABORT OFF 
GO
ALTER DATABASE [IndexData] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [IndexData] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [IndexData] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [IndexData] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [IndexData] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [IndexData] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [IndexData] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [IndexData] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [IndexData] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [IndexData] SET  DISABLE_BROKER 
GO
ALTER DATABASE [IndexData] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [IndexData] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [IndexData] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [IndexData] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [IndexData] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [IndexData] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [IndexData] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [IndexData] SET RECOVERY FULL 
GO
ALTER DATABASE [IndexData] SET  MULTI_USER 
GO
ALTER DATABASE [IndexData] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [IndexData] SET DB_CHAINING OFF 
GO
ALTER DATABASE [IndexData] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [IndexData] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [IndexData] SET DELAYED_DURABILITY = DISABLED 
GO
USE [IndexData]
GO
/****** Object:  Rule [DateOnly_Rule]    Script Date: 11/20/2018 3:33:21 PM ******/
CREATE RULE [dbo].[DateOnly_Rule] 
AS
(@date = convert (char(8), @date, 112))

GO
/****** Object:  UserDefinedDataType [dbo].[DateOnly_Type]    Script Date: 11/20/2018 3:33:21 PM ******/
CREATE TYPE [dbo].[DateOnly_Type] FROM [datetime] NOT NULL
GO
/****** Object:  Table [dbo].[Clients]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Clients](
	[ClientID] [varchar](30) NOT NULL,
	[ClientName] [varchar](75) NOT NULL,
	[City] [varchar](20) NOT NULL,
	[State] [int] NOT NULL,
	[Zip] [varchar](10) NOT NULL,
	[RM] [varchar](12) NOT NULL,
	[AddDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSecurityMasterFull]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSecurityMasterFull](
	[id] [int] IDENTITY(100000,1) NOT NULL,
	[Ticker] [varchar](10) NOT NULL,
	[Cusip] [varchar](8) NOT NULL,
	[Vendor] [varchar](1) NOT NULL,
	[CompanyName] [varchar](30) NULL,
	[SectorCode] [varchar](8) NULL,
	[BeginDate] [smalldatetime] NULL,
	[EndDate] [smalldatetime] NULL,
	[dateModified]  AS (getdate()),
 CONSTRAINT [PK_HistoricalSecurityMasterFull] PRIMARY KEY CLUSTERED 
(
	[Ticker] ASC,
	[Cusip] ASC,
	[Vendor] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSymbolChanges]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSymbolChanges](
	[ChangeDate] [datetime] NOT NULL,
	[OldSymbol] [varchar](10) NOT NULL,
	[NewSymbol] [varchar](10) NOT NULL,
	[CompanyName] [varchar](30) NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[Holidays]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Holidays](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[HDate] [datetime] NOT NULL,
	[Vendor] [varchar](50) NOT NULL,
	[Country] [varchar](50) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[Jobs]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Jobs](
	[ClientID] [varchar](30) NOT NULL,
	[JobName] [varchar](80) NOT NULL,
	[JobType] [varchar](10) NULL,
	[Schedule] [varchar](12) NOT NULL,
	[LastProcessDate] [dbo].[DateOnly_Type] NULL,
	[LastFilterFromDate] [dbo].[DateOnly_Type] NULL,
	[LastFilterToDate] [dbo].[DateOnly_Type] NULL,
	[Active] [varchar](3) NOT NULL,
	[Vendor] [varchar](50) NOT NULL,
	[InputFormat] [varchar](50) NULL,
	[RefReport] [varchar](50) NULL,
	[DataSet] [varchar](50) NULL,
	[WorkflowStatus] [varchar](50) NULL,
	[WorkflowStatusDatetime] [datetime] NULL,
	[AddDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[ProcessStatus]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ProcessStatus](
	[ProcessDate] [date] NOT NULL,
	[Vendor] [varchar](50) NOT NULL,
	[Dataset] [varchar](50) NOT NULL,
	[IndexName] [varchar](20) NOT NULL,
	[OpenData] [varchar](1) NOT NULL,
	[CloseData] [varchar](1) NOT NULL,
	[TotalReturnData] [varchar](1) NOT NULL,
	[SecurityMasterData] [varchar](1) NOT NULL,
	[SymbolChangeData] [varchar](1) NOT NULL,
	[AxmlConstituentData] [varchar](1) NOT NULL,
	[AxmlSectorData] [varchar](1) NOT NULL,
	[ExpectedClientFiles] [int] NOT NULL,
	[ActualClientFiles] [int] NOT NULL,
 CONSTRAINT [PK_ProcessStatus_1] PRIMARY KEY CLUSTERED 
(
	[ProcessDate] ASC,
	[Vendor] ASC,
	[Dataset] ASC,
	[IndexName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[RussellDailyHoldings1]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[RussellDailyHoldings1](
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[CUSIP] [varchar](9) NOT NULL,
	[Ticker] [varchar](7) NOT NULL,
	[MktValue] [varchar](13) NOT NULL,
	[SharesDenominator] [int] NOT NULL,
	[Sector] [varchar](7) NOT NULL,
	[SecurityReturn] [varchar](7) NULL,
 CONSTRAINT [PK_RussellHoldings1] PRIMARY KEY CLUSTERED 
(
	[FileDate] ASC,
	[CUSIP] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[RussellDailyHoldings2]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[RussellDailyHoldings2](
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[CUSIP] [varchar](9) NOT NULL,
	[IndexName] [varchar](9) NOT NULL,
	[SharesNumerator] [int] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[RussellDailyIndexReturns]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[RussellDailyIndexReturns](
	[IndexName] [varchar](9) NOT NULL,
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[TotalReturn] [varchar](12) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[RussellDailyTotals]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[RussellDailyTotals](
	[FileType] [varchar](50) NOT NULL,
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[FileName] [varchar](80) NOT NULL,
	[VendorTotal] [int] NOT NULL,
	[AdventTotal] [int] NOT NULL,
	[ZeroSharesTotal] [int] NOT NULL,
	[dateModified] [datetime] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SnpDailyClosingHoldings]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SnpDailyClosingHoldings](
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[IndexCode] [varchar](8) NOT NULL,
	[StockKey] [varchar](7) NOT NULL,
	[EffectiveDate] [dbo].[DateOnly_Type] NOT NULL,
	[CUSIP] [varchar](10) NULL,
	[Ticker] [varchar](8) NULL,
	[GicsCode] [varchar](9) NULL,
	[MarketCap] [varchar](30) NULL,
	[Weight] [varchar](30) NULL,
	[TotalReturn] [varchar](30) NULL,
 CONSTRAINT [PK_SnpDailyHoldings2] PRIMARY KEY CLUSTERED 
(
	[FileDate] ASC,
	[IndexCode] ASC,
	[StockKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SnpDailyIndexReturns]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SnpDailyIndexReturns](
	[IndexName] [varchar](9) NOT NULL,
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[TotalReturn] [varchar](24) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SnpDailyOpeningHoldings]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SnpDailyOpeningHoldings](
	[FileDate] [dbo].[DateOnly_Type] NOT NULL,
	[IndexCode] [varchar](8) NOT NULL,
	[StockKey] [varchar](7) NOT NULL,
	[EffectiveDate] [dbo].[DateOnly_Type] NOT NULL,
	[CUSIP] [varchar](10) NULL,
	[Ticker] [varchar](8) NULL,
	[GicsCode] [varchar](9) NULL,
	[MarketCap] [varchar](30) NULL,
	[Weight] [varchar](30) NULL,
 CONSTRAINT [PK_SnpDailyHoldings] PRIMARY KEY CLUSTERED 
(
	[FileDate] ASC,
	[IndexCode] ASC,
	[StockKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SystemSettings]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SystemSettings](
	[SettingName] [varchar](50) NOT NULL,
	[SettingValue] [varchar](80) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[TotalReturns]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[TotalReturns](
	[IndexName] [varchar](20) NOT NULL,
	[ReturnDate] [datetime] NOT NULL,
	[Vendor] [varchar](50) NOT NULL,
	[VendorFormat] [varchar](50) NOT NULL,
	[VendorReturn] [float] NULL,
	[AdvReturn] [float] NULL,
	[AdvAdjFactor] [float] NULL,
	[AdvReturnAdj] [float] NULL,
	[AdvReturnDb] [float] NULL,
	[Diff] [float] NULL,
	[DiffAdj] [float] NULL,
	[DiffDb] [float] NULL,
	[CumltDiff] [float] NULL,
	[dateModified] [datetime] NULL,
	[ClientId] [varchar](50) NULL,
	[RequestFile] [varchar](200) NULL,
 CONSTRAINT [PK_TotalReturns] PRIMARY KEY CLUSTERED 
(
	[IndexName] ASC,
	[ReturnDate] ASC,
	[Vendor] ASC,
	[VendorFormat] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[VendorIndexMap]    Script Date: 11/20/2018 3:33:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[VendorIndexMap](
	[Vendor] [varchar](50) NOT NULL,
	[Dataset] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_Dataset]  DEFAULT (''),
	[VendorIndexName] [varchar](20) NOT NULL,
	[AdventIndexName] [varchar](20) NOT NULL,
	[IndexSpec] [char](1) NOT NULL,
	[Supported] [varchar](3) NOT NULL,
	[AxmlSectorFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlSectorFile]  DEFAULT (''),
	[AxmlConstituentFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlConstituentFile]  DEFAULT ('')
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_Table_1_OpenFiles]  DEFAULT ('') FOR [OpenData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_Table_1_CloseFiles]  DEFAULT ('') FOR [CloseData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_TotalReturnData]  DEFAULT ('') FOR [TotalReturnData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_SecurityMasterData]  DEFAULT ('') FOR [SecurityMasterData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_SymbolChangeData]  DEFAULT ('') FOR [SymbolChangeData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_AxmlData]  DEFAULT ('') FOR [AxmlConstituentData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_AxmlSectorData]  DEFAULT ('') FOR [AxmlSectorData]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_ExpectedClientFiles]  DEFAULT ('0') FOR [ExpectedClientFiles]
GO
ALTER TABLE [dbo].[ProcessStatus] ADD  CONSTRAINT [DF_ProcessStatus_ActualClientFiles]  DEFAULT ('0') FOR [ActualClientFiles]
GO
ALTER TABLE [dbo].[RussellDailyTotals] ADD  CONSTRAINT [DF_RussellDailyTotals_dateModified]  DEFAULT (getdate()) FOR [dateModified]
GO
ALTER TABLE [dbo].[TotalReturns] ADD  CONSTRAINT [DF_TotalReturns_Vendor]  DEFAULT ('') FOR [Vendor]
GO
USE [master]
GO
ALTER DATABASE [IndexData] SET  READ_WRITE 
GO
