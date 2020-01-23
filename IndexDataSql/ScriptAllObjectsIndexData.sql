USE [master]
GO
/****** Object:  Database [AdvIndexData]    Script Date: 1/22/2020 11:21:12 AM ******/
CREATE DATABASE [AdvIndexData]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'IndexData', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData.mdf' , SIZE = 3390848KB , MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
 LOG ON 
( NAME = N'IndexData_log', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData_log.ldf' , SIZE = 22144KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO
ALTER DATABASE [AdvIndexData] SET COMPATIBILITY_LEVEL = 120
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [AdvIndexData].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [AdvIndexData] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [AdvIndexData] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [AdvIndexData] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [AdvIndexData] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [AdvIndexData] SET ARITHABORT OFF 
GO
ALTER DATABASE [AdvIndexData] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [AdvIndexData] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [AdvIndexData] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [AdvIndexData] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [AdvIndexData] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [AdvIndexData] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [AdvIndexData] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [AdvIndexData] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [AdvIndexData] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [AdvIndexData] SET  DISABLE_BROKER 
GO
ALTER DATABASE [AdvIndexData] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [AdvIndexData] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [AdvIndexData] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [AdvIndexData] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [AdvIndexData] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [AdvIndexData] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [AdvIndexData] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [AdvIndexData] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [AdvIndexData] SET  MULTI_USER 
GO
ALTER DATABASE [AdvIndexData] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [AdvIndexData] SET DB_CHAINING OFF 
GO
ALTER DATABASE [AdvIndexData] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [AdvIndexData] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [AdvIndexData] SET DELAYED_DURABILITY = DISABLED 
GO
USE [AdvIndexData]
GO
/****** Object:  Rule [DateOnly_Rule]    Script Date: 1/22/2020 11:21:12 AM ******/
CREATE RULE [dbo].[DateOnly_Rule] 
AS
(@date = convert (char(8), @date, 112))

GO
/****** Object:  UserDefinedDataType [dbo].[DateOnly_Type]    Script Date: 1/22/2020 11:21:12 AM ******/
CREATE TYPE [dbo].[DateOnly_Type] FROM [datetime] NOT NULL
GO
/****** Object:  Table [dbo].[AuditTrail]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[AuditTrail](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ConsoleUser] [varchar](50) NOT NULL,
	[ClientID] [varchar](30) NOT NULL,
	[Message] [varchar](2000) NOT NULL,
	[AddDate] [datetime] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[AxmlOutput]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[AxmlOutput](
	[Source] [varchar](4) NOT NULL CONSTRAINT [DF_AxmlOutput_Source]  DEFAULT (''),
	[IndexName] [varchar](9) NOT NULL CONSTRAINT [DF_AxmlOutput_IndexName]  DEFAULT (''),
	[ReturnDate] [date] NOT NULL CONSTRAINT [DF_AxmlOutput_ReturnDate]  DEFAULT (''),
	[OutputType] [varchar](20) NOT NULL CONSTRAINT [DF_AxmlOutput_VendorFormat]  DEFAULT (''),
	[Identifier] [varchar](12) NOT NULL CONSTRAINT [DF_AxmlOutput_Identifier]  DEFAULT (''),
	[OutputSubType] [varchar](20) NOT NULL CONSTRAINT [DF_AxmlOutput_OutputSubType]  DEFAULT (''),
	[Weight] [varchar](14) NOT NULL CONSTRAINT [DF_AxmlOutput_Weight]  DEFAULT (''),
	[Irr] [varchar](14) NOT NULL CONSTRAINT [DF_AxmlOutput_Irr]  DEFAULT (''),
 CONSTRAINT [PK_AxmlOutput] PRIMARY KEY CLUSTERED 
(
	[Source] ASC,
	[IndexName] ASC,
	[ReturnDate] ASC,
	[OutputType] ASC,
	[Identifier] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[Clients]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[Countries]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Countries](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Country] [varchar](50) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalCusipChanges]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalCusipChanges](
	[changeDate] [datetime] NOT NULL,
	[oldSymbol] [varchar](20) NOT NULL,
	[oldDescription] [varchar](200) NULL,
	[newSymbol] [varchar](20) NULL,
	[newDescription] [varchar](200) NULL,
	[sourcefile] [varchar](300) NULL,
	[dateModified] [datetime] NULL,
	[CreationDate] [datetime] NULL,
 CONSTRAINT [PK_HistoricalCusipChanges] PRIMARY KEY CLUSTERED 
(
	[changeDate] ASC,
	[oldSymbol] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSecurityMaster]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSecurityMaster](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[Ticker] [varchar](20) NOT NULL,
	[Cusip] [varchar](20) NOT NULL,
	[Description] [varchar](200) NULL,
	[BeginningDate] [smalldatetime] NOT NULL,
	[EndDate] [smalldatetime] NULL,
	[sourcefile] [varchar](300) NULL,
	[dateModified] [datetime] NULL CONSTRAINT [DF__Historica__dateM__2FBA0BF1]  DEFAULT (getdate()),
 CONSTRAINT [PK_HistoricalSecurityMaster] PRIMARY KEY CLUSTERED 
(
	[Cusip] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSecurityMasterFull]    Script Date: 1/22/2020 11:21:12 AM ******/
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
	[StockKey] [varchar](10) NULL,
	[CompanyName] [varchar](100) NULL,
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
/****** Object:  Table [dbo].[HistoricalSecurityMasterFullChanges]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSecurityMasterFullChanges](
	[id] [int] NOT NULL,
	[ProcessDate] [smalldatetime] NULL,
	[ChangeType] [varchar](6) NOT NULL,
	[Cusip] [varchar](8) NULL,
	[CusipNew] [varchar](8) NULL,
	[Ticker] [varchar](10) NULL,
	[TickerNew] [varchar](10) NULL,
	[CompanyName] [varchar](100) NULL,
	[CompanyNameNew] [varchar](100) NULL,
	[SectorCode] [varchar](8) NULL,
	[SectorCodeNew] [varchar](8) NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSecurityMasterFullCopy]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSecurityMasterFullCopy](
	[id] [int] NOT NULL,
	[Ticker] [varchar](10) NOT NULL,
	[Cusip] [varchar](8) NOT NULL,
	[Vendor] [varchar](1) NOT NULL,
	[StockKey] [varchar](10) NULL,
	[CompanyName] [varchar](100) NULL,
	[SectorCode] [varchar](8) NULL,
	[BeginDate] [smalldatetime] NULL,
	[EndDate] [smalldatetime] NULL,
	[dateModified]  AS (getdate()),
 CONSTRAINT [PK_HistoricalSecurityMasterFullCopy] PRIMARY KEY CLUSTERED 
(
	[Ticker] ASC,
	[Cusip] ASC,
	[Vendor] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HistoricalSymbolChanges]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[HistoricalSymbolChanges](
	[Vendor] [varchar](1) NOT NULL,
	[ChangeDate] [datetime] NOT NULL,
	[OldSymbol] [varchar](10) NOT NULL,
	[NewSymbol] [varchar](10) NOT NULL,
	[CompanyName] [varchar](30) NULL,
 CONSTRAINT [PK_HistoricalSymbolChanges] PRIMARY KEY CLUSTERED 
(
	[Vendor] ASC,
	[ChangeDate] ASC,
	[OldSymbol] ASC,
	[NewSymbol] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[Holidays]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[IndexMatrix]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[IndexMatrix](
	[Vendor] [varchar](40) NOT NULL,
	[JobName] [varchar](80) NOT NULL,
	[IndexLevel] [varchar](20) NOT NULL,
	[IndexName] [varchar](20) NOT NULL,
	[IndexClientName] [varchar](20) NOT NULL,
	[IndexDescription] [varchar](80) NOT NULL,
	[JobType] [varchar](6) NOT NULL,
	[Schedule] [varchar](5) NOT NULL,
	[InputFormat] [varchar](50) NULL,
	[RefReport] [varchar](50) NULL,
	[DataSet] [varchar](50) NULL,
	[FileNamePattern] [varchar](40) NOT NULL,
	[FileNameSuffix] [varchar](3) NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[JobIndexIds]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[JobIndexIds](
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
/****** Object:  Table [dbo].[Jobs]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[ProcessStatus]    Script Date: 1/22/2020 11:21:12 AM ******/
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
	[OpenData] [varchar](1) NOT NULL CONSTRAINT [DF_Table_1_OpenFiles]  DEFAULT (''),
	[CloseData] [varchar](1) NOT NULL CONSTRAINT [DF_Table_1_CloseFiles]  DEFAULT (''),
	[TotalReturnData] [varchar](1) NOT NULL CONSTRAINT [DF_ProcessStatus_TotalReturnData]  DEFAULT (''),
	[SecurityMasterData] [varchar](1) NOT NULL CONSTRAINT [DF_ProcessStatus_SecurityMasterData]  DEFAULT (''),
	[SymbolChangeData] [varchar](1) NOT NULL CONSTRAINT [DF_ProcessStatus_SymbolChangeData]  DEFAULT (''),
	[AxmlConstituentData] [varchar](1) NOT NULL CONSTRAINT [DF_ProcessStatus_AxmlData]  DEFAULT (''),
	[AxmlSectorData] [varchar](1) NOT NULL CONSTRAINT [DF_ProcessStatus_AxmlSectorData]  DEFAULT (''),
	[ExpectedConstituentClientFiles] [int] NOT NULL CONSTRAINT [DF_ProcessStatus_ExpectedClientFiles]  DEFAULT ('0'),
	[ActualConstituentClientFiles] [int] NOT NULL CONSTRAINT [DF_ProcessStatus_ActualClientFiles]  DEFAULT ('0'),
	[ExpectedSectorClientFiles] [int] NOT NULL CONSTRAINT [DF_ProcessStatus_ExpectedSectorClientFiles]  DEFAULT ('0'),
	[ActualSectorClientFiles] [int] NOT NULL CONSTRAINT [DF_ProcessStatus_ActualSectorClientFiles]  DEFAULT ('0'),
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
/****** Object:  Table [dbo].[RussellDailyHoldings1]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[RussellDailyHoldings2]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[RussellDailyIndexReturns]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[RussellDailyTotals]    Script Date: 1/22/2020 11:21:12 AM ******/
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
	[dateModified] [datetime] NOT NULL CONSTRAINT [DF_RussellDailyTotals_dateModified]  DEFAULT (getdate())
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SnpDailyClosingHoldings]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[SnpDailyIndexReturns]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[SnpDailyOpeningHoldings]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[States]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[States](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[State] [varchar](50) NOT NULL,
	[CountryID] [int] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SystemSettings]    Script Date: 1/22/2020 11:21:12 AM ******/
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
/****** Object:  Table [dbo].[TotalReturns]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[TotalReturns](
	[IndexName] [varchar](20) NOT NULL,
	[ReturnDate] [datetime] NOT NULL,
	[Vendor] [varchar](50) NOT NULL CONSTRAINT [DF_TotalReturns_Vendor]  DEFAULT (''),
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
/****** Object:  Table [dbo].[VendorIndexMap]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[VendorIndexMap](
	[Vendor] [varchar](50) NOT NULL,
	[Dataset] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_Dataset]  DEFAULT (''),
	[IndexName] [varchar](20) NOT NULL,
	[IndexClientName] [varchar](20) NOT NULL,
	[IndexSpec] [char](1) NOT NULL,
	[Supported] [varchar](3) NOT NULL,
	[AxmlSectorFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlSectorFile]  DEFAULT (''),
	[AxmlConstituentFile] [varchar](50) NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlConstituentFile]  DEFAULT (''),
	[AxmlBatchID] [int] NOT NULL CONSTRAINT [DF_VendorIndexMap_AxmlBatchID]  DEFAULT ((0)),
 CONSTRAINT [PK_VendorIndexMap] PRIMARY KEY CLUSTERED 
(
	[IndexName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  View [dbo].[v_ClientIndexes]    Script Date: 1/22/2020 11:21:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/******************************************************************************
-- Create v_ClientIndexes view
******************************************************************************/
CREATE VIEW [dbo].[v_ClientIndexes]
AS
SELECT DISTINCT 
                      TOP 100 PERCENT JI.ClientID, CL.ClientName, JI.Vendor, JI.VendorIndexName, JI.JobName, J.JobType, J.Schedule, J.InputFormat, J.RefReport, J.DataSet, 
                      v.AdventIndexName, im.IndexDescription
FROM         dbo.JobIndexIds AS JI INNER JOIN
                      dbo.Jobs AS J ON JI.ClientID = J.ClientID AND JI.JobName = J.JobName LEFT OUTER JOIN
                      dbo.Clients AS CL ON CL.ClientID = J.ClientID INNER JOIN
                      dbo.VendorIndexMap AS v ON JI.VendorIndexName = v.VendorIndexName INNER JOIN
                      dbo.IndexMatrix AS im ON JI.VendorIndexName = im.VendorIndexName
WHERE     (J.Active = 'Yes') AND (JI.ClientID <> 'SystemClient') AND (JI.ClientID <> 'IndexData')
ORDER BY JI.ClientID, CL.ClientName, JI.Vendor, JI.VendorIndexName, JI.JobName, J.JobType, J.Schedule, J.InputFormat, J.RefReport, J.DataSet


GO
USE [master]
GO
ALTER DATABASE [AdvIndexData] SET  READ_WRITE 
GO
