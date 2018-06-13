USE [master]
GO

/****** Object:  Database [IndexData]    Script Date: 6/13/2018 4:08:12 PM ******/
CREATE DATABASE [IndexData]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'IndexData', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData.mdf' , SIZE = 109504KB , MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
 LOG ON 
( NAME = N'IndexData_log', FILENAME = N'C:\A_Development\SQL\IndexData\Db\IndexData_log.ldf' , SIZE = 2160960KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
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

ALTER DATABASE [IndexData] SET  READ_WRITE 
GO

