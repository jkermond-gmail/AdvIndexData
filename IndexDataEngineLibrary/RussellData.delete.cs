using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Configuration;
using AdventUtilityLibrary;


namespace IndexDataEngineLibrary
{
    public class RussellData
    {
        private LogHelper logHelper;

        #region privates, enums, constants
        private StreamWriter swLogFile = null;
        private string LogFileName;
        private SqlConnection mSqlConn = null;
        private SqlDataReader mSqlDr = null;
        private int ConstituentCount = 0;
        private string mPrevId;
        private double mRolledUpWeight;
        private double mRolledUpReturn;
        private string mNumberFormat12 = "0.############";
        private string mNumberFormat4 = "0.####";
        //private string mConnectionString = "";
        private string mConnectionString = "server=VSTGMDDB2-1;database=IndexData;uid=sa;pwd=M@gichat!";


        private enum HoldingFileFormats
        {
            H_OPEN_RGS,
            H_CLOSE_RGS
        }

        private enum OutputTypes
        {
            ORIGINAL_TICKER,
            CURRENT_TICKER,
            SECTOR,
            SUBSECTOR,
            INDUSTRY
        }

        private int[] HoldingFileRecLengths = new int[]
        {
            605,    //  H_OPEN_RGS
            591     //  H_CLOSE_RGS
        };

        private const string TOTAL_COUNT = "Total Count:";

        #endregion End privates, enums, constants

        #region Production Code used by processor.process2()
        /***************************************************/

        #region Constructor / Finish 
        public RussellData()
        {
            //private const string SQL_CONN = "server=JKERMOND\\JKERMOND;database=AdvIndexData;uid=sa;pwd=M@gichat!";
            mConnectionString = ConfigurationManager.AppSettings["AdoConnectionString"];
            OpenLogFile();

        }

        public RussellData(LogHelper appLogHelper)
        {
            logHelper = appLogHelper;
            logHelper.Info("RussellData()", "RussellData");
        }

        public void SetConnectionString(string ConnectionString)
        {
            mConnectionString = ConnectionString;
        }


        private void OpenLogFile()
        {
            //private const string PATH = @"D:\IndexData\Russell\Test\";
            //private const string LOG_FILE = "RussellData.txt";
            LogFileName = ConfigurationManager.AppSettings["RussellLogFile"];
            //if (File.Exists(LogFileName))
            //    File.Delete(LogFileName);
            if (swLogFile == null)
            {
                if (!File.Exists(LogFileName))
                    swLogFile = File.CreateText(LogFileName);
                else
                    swLogFile = File.AppendText(LogFileName);

                //logHelper.WriteLine("Russell Data started: " + DateTime.Now);
                ////swLogFile.Flush();
            }
        }


        private string GetLogFileName()
        {
            return (LogFileName);
        }

        private void CloseLogFile()
        {
            if (swLogFile != null)
            {
                //swLogFile.Flush();
                swLogFile.Close();
                swLogFile = null;
            }
        }



        private void CloseGlobals()
        {
            if (mSqlDr != null)
            {
                mSqlDr.Close();
                mSqlDr = null;
            }
            if (mSqlConn != null)
            {
                mSqlConn.Close();
                mSqlConn = null;
            }
            //ConstituentCount = 0;
            mPrevId = "";
            mRolledUpWeight = 0.0;
            mRolledUpReturn = 0.0;

        }

        public void RussellData_Finish()
        {
            CloseLogFile();
            CloseGlobals();
        }
        #endregion End Constructor / Finish

        #region Daily Holdings File Processing

        public bool HoldingsFilesUpdated(DateTime ProcessDate)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            string sFileDate = "";
            bool bUpdated = false;
            DateTime OpenFileDate = DateTime.MinValue;
            DateTime CloseFileDate = DateTime.MinValue;
            int OpenVendorTotal = 0;
            int OpenAdventTotal = 0;
            int CloseVendorTotal = 0;
            int CloseAdventTotal = 0;

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT     TOP 1 FileDate, VendorTotal, AdventTotal
                    FROM         RussellDailyTotals
                    WHERE     (FileType = 'open')
                    ORDER BY FileDate desc
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sFileDate = dr["FileDate"].ToString();
                        OpenFileDate = DateTime.Parse(sFileDate);
                        OpenVendorTotal = (int)dr["VendorTotal"];
                        OpenAdventTotal = (int)dr["AdventTotal"];
                    }
                }
                dr.Close();

                SqlSelect = @"
                    SELECT     TOP 1 FileDate, VendorTotal, AdventTotal
                    FROM         RussellDailyTotals
                    WHERE     (FileType = 'close')
                    ORDER BY FileDate desc
                    ";
                cmd.CommandText = SqlSelect;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sFileDate = dr["FileDate"].ToString();
                        CloseFileDate = DateTime.Parse(sFileDate);
                        CloseVendorTotal = (int)dr["VendorTotal"];
                        CloseAdventTotal = (int)dr["AdventTotal"];
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }
            bool bDatesUpdated = ((OpenFileDate == ProcessDate) && (CloseFileDate == ProcessDate));
            bool bTotalsUpdated = ((OpenVendorTotal > 0) && (CloseVendorTotal > 0) &&
                                  (OpenAdventTotal > 0) && (CloseAdventTotal > 0));
            bool bOpenTotalsMatch = (OpenVendorTotal == OpenAdventTotal);
            bool bCloseTotalsMatch = (CloseVendorTotal == CloseAdventTotal);

            bUpdated = ((bDatesUpdated) && (bTotalsUpdated) && (bOpenTotalsMatch) && (bCloseTotalsMatch));
            if (!bUpdated)
            {
                logHelper.WriteLine("HoldingsFilesUpdated() == false");
                logHelper.WriteLine("Process Date      : " + ProcessDate.ToShortDateString());
                logHelper.WriteLine("Open File Date    : " + OpenFileDate.ToShortDateString());
                logHelper.WriteLine("Close File Date   : " + CloseFileDate.ToShortDateString());
                logHelper.WriteLine("Open Vendor Total : " + OpenVendorTotal.ToString());
                logHelper.WriteLine("Open Advent Total : " + OpenAdventTotal.ToString());
                logHelper.WriteLine("Close Vendor Total: " + CloseVendorTotal.ToString());
                logHelper.WriteLine("Close Advent Total: " + CloseAdventTotal.ToString());
            }
            return (bUpdated);
        }

        public void ProcessRussellHoldingsFiles(DateTime oStartDate, DateTime oEndDate, bool bOpenFiles, bool bCloseFiles)
        {
            DateTime oProcessDate;
            int DateCompare;
            string FilePath = @"d:\indexdata\vifs\russell\";
            //string FilePath = ConfigurationManager.AppSettings["RussellVifsPath"];
            string FileName;
            string sMsg = "ProcessRussellHoldingsFiles: ";

            logHelper.WriteLine(sMsg + "Started " + DateTime.Now);

            try
            {
                for (oProcessDate = oStartDate
                   ; (DateCompare = oProcessDate.CompareTo(oEndDate)) <= 0
                   ; oProcessDate = oProcessDate.AddDays(1))
                {
                    if (bOpenFiles)
                    {
                        FileName = FilePath + "H_OPEN_R3000E_" + oProcessDate.ToString("yyyyMMdd") + "_RGS.TXT";
                        if (File.Exists(FileName))
                        {
                            logHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            AddRussellOpeningData(HoldingFileFormats.H_OPEN_RGS, FileName, oProcessDate);
                            logHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }

                    if (bCloseFiles)
                    {
                        FileName = FilePath + "H_" + oProcessDate.ToString("yyyyMMdd") + "_RGS_R3000E.TXT";
                        if (File.Exists(FileName))
                        {
                            logHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            AddRussellClosingData(HoldingFileFormats.H_CLOSE_RGS, FileName, oProcessDate);
                            logHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }
                }
            }
            catch
            {
            }
            finally
            {
                logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                RussellData_Finish();
            }
        }

        private string GetField(string TextLine, int Start, int Length)
        {
            string sFld = TextLine.Substring(Start - 1, Length);
            sFld = sFld.Trim();
            return (sFld);
        }

        private void AddTotalCounts(
            DateTime FileDate, HoldingFileFormats FileFormat, string FileName,
            string TextLine, int SecuritiesTotal, int ZeroSharesTotal)
        {
            int Pos = TextLine.IndexOf(TOTAL_COUNT);
            int VendorTotal = Convert.ToInt32(TextLine.Substring(Pos + TOTAL_COUNT.Length));
            int AdventTotal = SecuritiesTotal - ZeroSharesTotal;
            logHelper.WriteLine(FileName + ": SecurityCount " + AdventTotal +
                                " VendorTotalCount " + VendorTotal + " Zero Shares " + ZeroSharesTotal);
            //CREATE TABLE dbo.RussellDailyTotals
            //(
            //    FileType        varchar(50)   COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
            //    FileDate        DateOnly_Type NOT NULL,
            //    FileName        varchar(80)   COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
            //    VendorTotal     int           NOT NULL,
            //    AdventTotal     int           NOT NULL,
            //    ZeroSharesTotal int           CONSTRAINT DF_RussellDailyTotals_ZeroSharesTotal DEFAULT 0 NOT NULL,
            //    dateModified    datetime      CONSTRAINT DF_RussellDailyTotals_dateModified DEFAULT getdate() NOT NULL,
            //    CONSTRAINT PK_RussellDailyTotals
            //    PRIMARY KEY CLUSTERED (FileType,FileDate)
            //)

            SqlConnection cnSql = new SqlConnection(mConnectionString);
            try
            {
                string sFileType = "";
                switch (FileFormat)
                {
                    case HoldingFileFormats.H_CLOSE_RGS:
                        sFileType = "Close";
                        break;
                    case HoldingFileFormats.H_OPEN_RGS:
                        sFileType = "Open";
                        break;
                }
                cnSql.Open();
                string SqlSelect = @"
                    select count(*) from RussellDailyTotals
                    where FileType = @FileType
                    and FileDate = @FileDate
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@FileType", SqlDbType.VarChar);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                cmd.Parameters["@FileType"].Value = sFileType;
                cmd.Parameters["@FileDate"].Value = FileDate;
                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 1)
                {
                    cmd.CommandText = @"
                        delete from RussellDailyTotals
                        where FileType = @FileType
                        and FileDate = @FileDate
                        ";
                    cmd.ExecuteNonQuery();
                }
                cmd.CommandText = @"
                    insert into RussellDailyTotals (FileType, FileDate, FileName, VendorTotal, AdventTotal, ZeroSharesTotal)
                    Values (@FileType, @FileDate, @FileName, @VendorTotal, @AdventTotal, @ZeroSharesTotal)
                    ";
                cmd.Parameters.Add("@FileName", SqlDbType.VarChar);
                cmd.Parameters.Add("@VendorTotal", SqlDbType.Int);
                cmd.Parameters.Add("@AdventTotal", SqlDbType.Int);
                cmd.Parameters.Add("@ZeroSharesTotal", SqlDbType.Int);
                cmd.Parameters["@FileName"].Value = FileName;
                cmd.Parameters["@VendorTotal"].Value = VendorTotal;
                cmd.Parameters["@AdventTotal"].Value = AdventTotal;
                cmd.Parameters["@ZeroSharesTotal"].Value = ZeroSharesTotal;
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    logHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                cnSql.Close();
            }



            return;
        }


        private void AddRussellOpeningData(HoldingFileFormats FileFormat, string FileName, DateTime FileDate)
        {
            SqlConnection cnSql1 = new SqlConnection(mConnectionString);
            SqlConnection cnSql2 = new SqlConnection(mConnectionString);
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmdHoldings = null;
            string Fld;
            DateTime oDate;
            StreamReader srHoldingsFile = null;

            cnSql1.Open();
            cnSql2.Open();

            SqlDelete = "delete FROM RussellDailyHoldings1 ";
            SqlWhere = "where FileDate = @FileDate";
            cmdHoldings = new SqlCommand();
            cmdHoldings.Connection = cnSql1;
            cmdHoldings.CommandText = SqlDelete + SqlWhere;
            cmdHoldings.Parameters.Add("@FileDate", SqlDbType.DateTime);
            cmdHoldings.Parameters["@FileDate"].Value = FileDate;
            cmdHoldings.ExecuteNonQuery();
            SqlDelete = "delete FROM RussellDailyHoldings2 ";
            cmdHoldings.CommandText = SqlDelete + SqlWhere;
            cmdHoldings.ExecuteNonQuery();


            string sTable1 = "RussellDailyHoldings1";
            string sTable2 = "RussellDailyHoldings2";

            SqlDataAdapter daAdvIndexData1 = new SqlDataAdapter(
                    "select * from " + sTable1 + " where FileDate = '" + FileDate + "'", cnSql1);
            SqlDataAdapter daAdvIndexData2 = new SqlDataAdapter(
                    "select * from " + sTable2 + " where FileDate = '" + FileDate + "'", cnSql2);
            // The following lines generates the required SQL INSERT Statement
            SqlCommandBuilder cbAdvIndexData1 = new SqlCommandBuilder(daAdvIndexData1);
            SqlCommandBuilder cbAdvIndexData2 = new SqlCommandBuilder(daAdvIndexData2);
            DataSet dsHoldings1 = new DataSet();
            DataSet dsHoldings2 = new DataSet();
            daAdvIndexData1.Fill(dsHoldings1, sTable1);
            daAdvIndexData2.Fill(dsHoldings2, sTable2);
            DataTable dtHoldings1 = dsHoldings1.Tables[sTable1];
            DataTable dtHoldings2 = dsHoldings2.Tables[sTable2];
            DataRow drHoldings1 = null;
            DataRow drHoldings2 = null;

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");
            string sSharesValue = "";
            string sSharesGrowth = "";
            string r1000 = "";
            string r2000 = "";
            string r2500 = "";
            string rmid = "";
            string rtop200 = "";
            string rsmallc = "";
            string r3000 = "";
            string rmicro = "";
            int SecurityCount = 0;
            int SharesDenominatorZeroCount = 0;
            bool FoundTotalCount = false;

            for (srHoldingsFile = new StreamReader(FileName)
               ; srHoldingsFile.EndOfStream == false
               ;)
            {
                bool add = false;
                bool IsHeader1 = false;
                bool IsHeader2 = false;

                TextLine = srHoldingsFile.ReadLine();

                if (!FoundTotalCount)
                    FoundTotalCount = TextLine.Contains(TOTAL_COUNT);

                if (TextLine.Length == HoldingFileRecLengths[(int)FileFormat] && !FoundTotalCount)
                {
                    IsHeader1 = (TextLine.StartsWith("Date") == true);
                    IsHeader2 = (TextLine.StartsWith("----") == true);
                    bool ok = (TextLine.StartsWith("20") == true) && !IsHeader1 && !IsHeader2;

                    if (ok)
                    {
                        drHoldings1 = dtHoldings1.NewRow();

                        Fld = GetField(TextLine, 1, 8);
                        if (Fld.Length == 8 && DateTime.TryParseExact(Fld, "yyyyMMdd", enUS, DateTimeStyles.None, out oDate))
                        {
                            Fld = oDate.ToString("MM/dd/yyyy");
                            drHoldings1["FileDate"] = Fld;
                        }
                        else
                            ok = false;

                        Fld = GetField(TextLine, 10, 8);
                        if (Fld.Length == 8)
                            drHoldings1["CUSIP"] = Fld;
                        else
                            ok = false;

                        Fld = GetField(TextLine, 33, 7);
                        if (Fld.Length >= 1)
                            drHoldings1["Ticker"] = Fld;
                        else
                            ok = false;
                        Fld = GetField(TextLine, 74, 13);
                        drHoldings1["MktValue"] = Fld;

                        Fld = GetField(TextLine, 102, 9);
                        drHoldings1["SharesDenominator"] = Fld;
                        if (Convert.ToInt32(Fld.ToString()) == 0)
                            SharesDenominatorZeroCount += 1;

                        sSharesValue = GetField(TextLine, 112, 9);
                        sSharesGrowth = GetField(TextLine, 122, 9);

                        r1000 = GetField(TextLine, 132, 1);
                        r2000 = GetField(TextLine, 134, 1);
                        r2500 = GetField(TextLine, 136, 1);
                        rmid = GetField(TextLine, 138, 1);
                        rtop200 = GetField(TextLine, 140, 1);
                        rsmallc = GetField(TextLine, 142, 1);
                        r3000 = GetField(TextLine, 144, 1);
                        rmicro = GetField(TextLine, 148, 1);

                        //Fld = GetField(TextLine, 164, 25);
                        //drHoldings1["CompanyName"] = Fld;

                        //Fld = GetField(TextLine, 190, 2);
                        //drHoldings1["Sector"] = Fld;

                        //Fld = GetField(TextLine, 193, 30);
                        //drHoldings1["SectorDesc"] = Fld;

                        //Fld = GetField(TextLine, 224, 4);
                        //drHoldings1["SubSector"] = Fld;

                        //Fld = GetField(TextLine, 229, 48);
                        //drHoldings1["SubSectorDesc"] = Fld;

                        Fld = GetField(TextLine, 278, 7);
                        drHoldings1["Sector"] = Fld;

                        //Fld = GetField(TextLine, 286, 48);
                        //drHoldings1["IndustryDesc"] = Fld;
                    }

                    if (ok)
                    {
                        if (r1000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r1000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r1000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r1000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (r2000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r2000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r2000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r2000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (r2500.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r2500";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r2500g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r2500v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (rmid.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rmid";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rmidg";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rmidv";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (rtop200.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rtop200";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rtop200g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rtop200v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (rsmallc.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rsmallc";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rsmallcg";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rsmallcv";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (r3000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r3000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r3000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r3000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if (rmicro.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rmicro";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if (Convert.ToInt32(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rmicrog";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if (Convert.ToInt32(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rmicrov";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }

                        SecurityCount += 1;
                        try
                        {
                            dtHoldings1.Rows.Add(drHoldings1);
                            add = true;

                        }
                        catch (SqlException ex)
                        {
                            if (ex.Number == 2627)
                            {
                                Console.WriteLine(ex.Message);
                                logHelper.WriteLine(FileName + ":" + TextLine);
                            }
                        }
                        finally
                        {
                        }
                    }
                }
                else if (FoundTotalCount)
                {
                    AddTotalCounts(FileDate, FileFormat, FileName, TextLine, SecurityCount, SharesDenominatorZeroCount);
                    break;
                }
                if (!add && !IsHeader1 && !IsHeader2 && (TextLine.Length > 0))
                {
                    logHelper.WriteLine("Skipping line:" + TextLine.ToString());
                }
            }
            try
            {
                daAdvIndexData1.Update(dtHoldings1);
                daAdvIndexData2.Update(dtHoldings2);
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    logHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
            srHoldingsFile.Close();
        }



        private void AddRussellClosingData(HoldingFileFormats FileFormat, string FileName, DateTime FileDate)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            cnSql.Open();
            string sTable = "RussellDailyHoldings1";
            string SqlUpdate = "update " + sTable + " set SecurityReturn = @SecurityReturn ";
            string SqlWhere = "where FileDate = @FileDate and CUSIP = @CUSIP ";
            SqlCommand cmdHoldingsRec = new SqlCommand(SqlUpdate + SqlWhere, cnSql);
            cmdHoldingsRec.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
            cmdHoldingsRec.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
            cmdHoldingsRec.Parameters.Add("@SecurityReturn", SqlDbType.VarChar, 7);

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            DateTime oDate = DateTime.MinValue;
            string sFileDate = "";
            string sCUSIP = "";
            string sSecurityReturn = "";
            string sShares = "";
            StreamReader srHoldingsFile = null;
            int SecurityCount = 0;
            int SharesDenominatorZeroCount = 0;

            bool FoundTotalCount = false;
            for (srHoldingsFile = new StreamReader(FileName)
               ; srHoldingsFile.EndOfStream == false
               ;)
            {
                bool add = false;
                bool IsHeader1 = false;
                bool IsHeader2 = false;

                TextLine = srHoldingsFile.ReadLine();

                if (!FoundTotalCount)
                    FoundTotalCount = TextLine.Contains(TOTAL_COUNT);

                if (TextLine.Length == HoldingFileRecLengths[(int)FileFormat] && !FoundTotalCount)
                {
                    IsHeader1 = (TextLine.StartsWith("Date") == true);
                    IsHeader2 = (TextLine.StartsWith("----") == true);
                    bool ok = (TextLine.StartsWith("20") == true) && !IsHeader1 && !IsHeader2;

                    if (ok)
                    {
                        sFileDate = GetField(TextLine, 1, 8);
                        if (sFileDate.Length == 8 && DateTime.TryParseExact(sFileDate, "yyyyMMdd", enUS, DateTimeStyles.None, out oDate))
                        {
                            sFileDate = oDate.ToString("MM/dd/yyyy");
                        }
                        else
                            ok = false;

                        sCUSIP = GetField(TextLine, 10, 8);
                        if (sCUSIP.Length != 8)
                            ok = false;

                        sSecurityReturn = GetField(TextLine, 55, 7);
                        sShares = GetField(TextLine, 88, 9);
                        if (Convert.ToInt32(sShares.ToString()) == 0)
                            SharesDenominatorZeroCount += 1;
                    }

                    if (ok)
                    {
                        SecurityCount += 1;
                        try
                        {
                            if (GetDailyHoldings1Count(oDate, sCUSIP) == 0)
                            {
                                string sOldCUSIP = GetOldCUSIP(sCUSIP);
                                if (sOldCUSIP.Length == 8)
                                {
                                    if (GetDailyHoldings1Count(oDate, sOldCUSIP) == 0)
                                    {
                                        logHelper.WriteLine("Skipping line:" + TextLine.ToString());
                                        logHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString() +
                                                            " linked to Old CUSIP " + sOldCUSIP.ToString());
                                        ok = false;
                                    }

                                    //throw new Exception("AddRussellClosingData can't update " + sCUSIP + " for date" + oDate.ToShortDateString());
                                    else
                                        sCUSIP = sOldCUSIP;
                                }
                                else if (sOldCUSIP.Length == 0)
                                {
                                    logHelper.WriteLine("Skipping line:" + TextLine.ToString());
                                    logHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString());
                                    ok = false;
                                }

                            }
                            if (ok)
                            {
                                cmdHoldingsRec.Parameters["@CUSIP"].Value = sCUSIP;
                                cmdHoldingsRec.Parameters["@FileDate"].Value = oDate;
                                cmdHoldingsRec.Parameters["@SecurityReturn"].Value = sSecurityReturn;
                                cmdHoldingsRec.ExecuteNonQuery();
                                add = true;
                            }
                        }

                        catch (SqlException ex)
                        {
                            if (ex.Number == 2627)
                            {
                                logHelper.WriteLine(ex.Message);
                                logHelper.WriteLine(FileName + ":" + TextLine);
                            }
                        }
                        finally
                        {
                        }
                    }
                }
                else if (FoundTotalCount)
                {
                    AddTotalCounts(FileDate, FileFormat, FileName, TextLine, SecurityCount, SharesDenominatorZeroCount);
                    break;
                }
                if (!add && !IsHeader1 && !IsHeader2 && (TextLine.Length > 0))
                {
                    logHelper.WriteLine("Skipping line:" + TextLine.ToString());
                }
            }
            srHoldingsFile.Close();
        }

        public int GetDailyHoldings1Count(DateTime FileDate, string sCUSIP)
        {
            SqlConnection conn = new SqlConnection(mConnectionString);
            int Count = 0;
            try
            {
                string SqlSelectCount = @"
                    select count(CUSIP) from RussellDailyHoldings1 where FileDate = @FileDate and CUSIP = @CUSIP 
                    ";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount, conn);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
                cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
                cmd.Parameters["@CUSIP"].Value = sCUSIP;
                cmd.Parameters["@FileDate"].Value = FileDate;
                Count = (int)cmd.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                conn.Close();
            }

            return (Count);
        }


        public string GetOldCUSIP(string sNewCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            string sOldCUSIP = "";

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT oldSymbol FROM HistoricalCusipChanges WHERE newSymbol = @newSymbol
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@newSymbol", SqlDbType.VarChar, 8);
                cmd.Parameters["@newSymbol"].Value = sNewCUSIP;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sOldCUSIP = dr["oldSymbol"].ToString();
                    }
                }

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }

            return (sOldCUSIP);
        }



        #endregion End Daily Holdings File Processing

        #region Get Security Return
        public string GetSecurityReturn(DateTime FileDate, string CUSIP)
        {
            SqlDataReader dr = null;
            string sSecurityReturn = "";

            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionString);
                    mSqlConn.Open();
                }

                string SqlSelect = @"
                    select SecurityReturn from  RussellDailyHoldings1
                    where FileDate = @FileDate and CUSIP = @CUSIP
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
                cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
                cmd.Parameters["@FileDate"].Value = FileDate;
                cmd.Parameters["@CUSIP"].Value = CUSIP;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sSecurityReturn = dr["SecurityReturn"].ToString();
                    }
                }
                else
                {
                    string msg = "no row found RussellDailyHoldings1: " + FileDate.ToShortDateString() + "CUSIP: " + CUSIP;
                    //ConsoleWriter.cWriteInfo(msg);
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
            }

            return (sSecurityReturn);
        }
        #endregion End Get Security Return

        #region Security Master
        public void AddSecurityMasterFull(string Ticker, string Cusip, string Description, string IndustryGroup, string Vendor, string IndexId, string EndDate)
        {
            /*
            if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[HistoricalSecurityMasterFull]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
            drop table [dbo].[HistoricalSecurityMasterFull]
            GO

            CREATE TABLE [dbo].[HistoricalSecurityMasterFull] (
                [id] [int] IDENTITY (100000, 1) NOT NULL ,
                [Ticker] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,
                [Cusip] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,
                [Description] [varchar] (80) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
                [IndustryGroup] [varchar] (80) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
                [Vendor] [varchar] (20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,
                [IndexId] [varchar] (20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL ,
                [BeginDate] [smalldatetime] NULL ,
                [EndDate] [smalldatetime] NULL ,
                [Sourcefile] [varchar] (300) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
                [dateModified] AS (getdate()) 
            ) ON [PRIMARY]
            GO

            ALTER TABLE [dbo].[HistoricalSecurityMasterFull] WITH NOCHECK ADD 
                CONSTRAINT [PK_HistoricalSecurityMasterFull] PRIMARY KEY  CLUSTERED 
                (
                    [Ticker],
                    [Cusip],
                    [Vendor],
                    [IndexId]
                )  ON [PRIMARY] 
            GO

            ALTER TABLE [dbo].[HistoricalSecurityMasterFull] ADD 
                CONSTRAINT [DF_HistoricalSecurityMasterFull_Sourcefile] DEFAULT ('') FOR [Sourcefile]
            GO


             */
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionString);
                    mSqlConn.Open();
                }
                CultureInfo enUS = new CultureInfo("en-US");

                DateTime oDate = DateTime.MinValue;
                DateTime.TryParseExact(EndDate, "yyyyMMdd", enUS, DateTimeStyles.None, out oDate);

                string SqlSelect = @"
                    select count(*) from HistoricalSecurityMasterFull
                    where Ticker = @Ticker
                    and Cusip = @Cusip
                    and Vendor = @Vendor
                    and IndexId = @IndexId
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@Ticker", SqlDbType.VarChar);
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters.Add("@IndexId", SqlDbType.VarChar);
                cmd.Parameters["@Ticker"].Value = Ticker;
                cmd.Parameters["@Cusip"].Value = Cusip;
                cmd.Parameters["@Vendor"].Value = Vendor;
                cmd.Parameters["@IndexId"].Value = IndexId;
                int iCount = (int)cmd.ExecuteScalar();

                if (iCount == 0)
                {
                    cmd.Parameters.Add("@BeginDate", SqlDbType.DateTime);
                    cmd.Parameters["@BeginDate"].Value = oDate;
                    cmd.CommandText =
                        "insert into HistoricalSecurityMasterFull (Ticker, Cusip, Vendor, IndexId, Description, IndustryGroup, BeginDate, EndDate) " +
                        "Values ( @Ticker, @Cusip, @Vendor, @IndexId, @Description, @IndustryGroup, @BeginDate, @EndDate ) ";
                }
                else
                {
                    cmd.CommandText =
                        "update HistoricalSecurityMasterFull  set " +
                        "Description = @Description, " +
                        "IndustryGroup = @IndustryGroup, " +
                        "EndDate = @EndDate " +
                        "where Ticker = @Ticker and Cusip = @Cusip and Vendor = @Vendor and IndexId = @IndexId";
                }
                cmd.Parameters.Add("@Description", SqlDbType.VarChar);
                cmd.Parameters["@Description"].Value = Description;
                cmd.Parameters.Add("@IndustryGroup", SqlDbType.VarChar);
                cmd.Parameters["@IndustryGroup"].Value = IndustryGroup;
                cmd.Parameters.Add("@EndDate", SqlDbType.DateTime);
                cmd.Parameters["@EndDate"].Value = oDate;
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }

        }

        public void GenerateHistSecMasterLists(string sListDate)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionString);
                    mSqlConn.Open();
                }
                string SqlSelectCount = "select count(*) from JobIndexIDs ";
                string SqlSelect = "select * from JobIndexIDs ";
                string SqlWhere = "WHERE RunSecMasterList = 'Yes' ";
                string SqlOrderBy = "order by ClientID, Vendor, IndexName";

                SqlCommand cmd = new SqlCommand(SqlSelectCount + SqlWhere, mSqlConn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    // todo-jk ConsoleWriter.cWriteInfo("Generating " + count + " Historical Security Master files. ");
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    SqlDataReader dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        i += 1;

                        string LogFileName = GetLogFileName();
                        CloseLogFile();
                        if (File.Exists(LogFileName))
                            File.Delete(LogFileName);
                        OpenLogFile();
                        string IndexName = dr["IndexName"].ToString();
                        // todo-jk IndexName = Utility.getClientIndexName(IndexName);
                        // todo-jk ConsoleWriter.cWriteInfo("Generating Historical Security Master files for " + dr["Vendor"].ToString() + " " +
                        //    IndexName + " starting " + dr["HistoryBeginDate"].ToString() + " for ClientID " + dr["ClientID"].ToString());

                        GenerateHistSecMasterList(
                            dr["ClientID"].ToString(),
                            dr["Vendor"].ToString(),
                            IndexName.ToString(),
                            dr["HistoryBeginDate"].ToString());
                        string BeginDate = dr["HistoryBeginDate"].ToString();
                        CultureInfo enUS = new CultureInfo("en-US");

                        DateTime oDate = DateTime.MinValue;
                        bool parsed = DateTime.TryParseExact(BeginDate.ToString(), "MM/dd/yyyy hh:mm:ss tt", enUS, DateTimeStyles.None, out oDate);
                        BeginDate = oDate.ToString("yyyyMMdd");
                        CloseLogFile();
                        string CopyToFileName = "HistSecMaster_" +
                                                IndexName.ToString() + "_" + BeginDate.ToString() + ".txt";
                        File.Copy(LogFileName, @"D:\IndexData\Russell\Test\" + CopyToFileName, true);
                        // todo-jk string sFTPDriveLetter = Utility.getSystemSettingValue("FTPDriveLetter");
                        // todo-jk string sFTPRoot = Utility.getSystemSettingValue("FTPRoot");
                        // todo-jk string sFTPClientFolder = Utility.getSystemSettingValue("FTPClientFolder");
                        // todo-jk string sFTPFoldername =
                        // todo-jk sFTPDriveLetter + "\\" + sFTPRoot + "\\" + dr["ClientID"].ToString() + "\\" + sFTPClientFolder + "\\";
                        // todo-jk ConsoleWriter.cWriteInfo("Copying Historical Security Master file " + CopyToFileName + " to " + sFTPFoldername);
                        // todo-jk File.Copy(LogFileName, sFTPFoldername + CopyToFileName, true);
                        // todo-jk ConsoleWriter.cWriteInfo("Successfully copied Historical Security Master file " + CopyToFileName + " to " + sFTPFoldername);
                        OpenLogFile();
                    }
                    dr.Close();
                    cmd.CommandText = @"update JobIndexIDs set RunSecMasterList = 'No', RunSecMasterListDate = '" + sListDate + @"' " +
                                      @"where RunSecMasterList = 'Yes'";
                    cmd.ExecuteNonQuery();

                }

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
        }

        public void GenerateHistSecMasterList(string sClientID, string sVendor, string sIndexId, string sBeginDate)
        {
            /*
            SELECT     sm.Ticker, sm.Cusip, sm.Description, sm.Vendor, sm.IndexId, sm.BeginDate, sm.EndDate, sc.SectorCode, sc.SectorDesc, sc.IndustryGroupCode, 
                                  sc.IndustryGroupDesc, sc.IndustryCode, sc.IndustryDesc, sc.SubIndustryCode, sc.SubIndustryDesc
            FROM         HistoricalSecurityMasterFull sm INNER JOIN
                                  SectorCodesGICS sc ON sm.IndustryGroup = sc.SubIndustryCode
            WHERE     (sm.Vendor = 'StandardAndPoors') AND (sm.IndexId = 'sp500') AND (sm.BeginDate >= '11/01/2012') AND (sm.EndDate <= '01/24/2014')
            ORDER BY sm.Ticker 
             * 
            SELECT     sm.Ticker, sm.Cusip, sm.Description, sm.Vendor, sm.IndexId, sm.BeginDate, sm.EndDate, sc.SectorCode, sc.SectorDesc, sc.SubSectorCode, 
                                  sc.SubSectorDesc, sc.IndustryCode, sc.IndustryDesc
            FROM         HistoricalSecurityMasterFull sm INNER JOIN
                                  SectorCodesRGS sc ON sm.IndustryGroup = sc.IndustryCode
            WHERE     (sm.Vendor = 'Russell') AND (sm.IndexId = 'r3000') AND (sm.BeginDate >= '11/01/2000') AND (sm.EndDate <= '01/24/2014')
            ORDER BY sm.Ticker 
             * * 
             */

            try
            {
                SqlConnection SqlConn = new SqlConnection(mConnectionString);
                SqlConn.Open();

                string SqlSelect = null;

                if (sVendor.Equals("StandardAndPoors"))
                {
                    SqlSelect = @"
                        SELECT  sm.Ticker, sm.Cusip, sm.Description, sm.Vendor, sm.IndexId, sm.BeginDate, sm.EndDate, 
                                sc.SectorCode, sc.SectorDesc, sc.IndustryGroupCode, sc.IndustryGroupDesc, 
                                sc.IndustryCode, sc.IndustryDesc, sc.SubIndustryCode, sc.SubIndustryDesc
                        FROM    HistoricalSecurityMasterFull sm 
                        INNER JOIN
                                SectorCodesGICS sc ON sm.IndustryGroup = sc.SubIndustryCode ";
                }
                else if (sVendor.Equals("Russell"))
                {
                    SqlSelect = @"
                        SELECT  sm.Ticker, sm.Cusip, sm.Description, sm.Vendor, sm.IndexId, sm.BeginDate, sm.EndDate, 
                                sc.SectorCode, sc.SectorDesc, sc.SubSectorCode, sc.SubSectorDesc, sc.IndustryCode, sc.IndustryDesc
                        FROM    HistoricalSecurityMasterFull sm 
                        INNER JOIN
                                SectorCodesRGS sc ON sm.IndustryGroup = sc.IndustryCode 
                                ";
                }

                string SqlWhere = @" 
                    WHERE (sm.Vendor = @Vendor) AND (sm.IndexId = @IndexId) AND (sm.EndDate >= @BeginDate) 
                    ";
                string SqlOrderBy = @"
                    ORDER BY sm.Ticker 
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere + SqlOrderBy, SqlConn);

                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters.Add("@IndexId", SqlDbType.VarChar);
                cmd.Parameters.Add("@BeginDate", SqlDbType.DateTime);
                cmd.Parameters["@Vendor"].Value = sVendor;
                cmd.Parameters["@IndexId"].Value = sIndexId;
                cmd.Parameters["@BeginDate"].Value = sBeginDate;

                SqlDataReader dr = cmd.ExecuteReader();
                int i = 0;
                string sLine = null;
                while (dr.Read())
                {
                    i += 1;
                    CultureInfo enUS = new CultureInfo("en-US");
                    DateTime oDate = DateTime.MinValue;
                    string BeginDate = dr["BeginDate"].ToString();
                    bool parsed = DateTime.TryParseExact(BeginDate.ToString(), "M/d/yyyy hh:mm:ss tt", enUS, DateTimeStyles.None, out oDate);
                    BeginDate = oDate.ToString("MM/dd/yyyy");
                    oDate = DateTime.MinValue;
                    string EndDate = dr["EndDate"].ToString();
                    parsed = DateTime.TryParseExact(EndDate.ToString(), "M/d/yyyy hh:mm:ss tt", enUS, DateTimeStyles.None, out oDate);
                    EndDate = oDate.ToString("MM/dd/yyyy");

                    sLine =
                        "\"" + dr["Ticker"].ToString() + "\"," + "\"" + dr["Cusip"].ToString() + "\"," +
                        "\"" + dr["Description"].ToString() + "\"," + "\"" + dr["Vendor"].ToString() + "\"," +
                        "\"" + dr["IndexId"].ToString() + "\"," + "\"" + BeginDate.ToString() + "\"," +
                        "\"" + EndDate.ToString() + "\",";
                    if (sVendor.Equals("StandardAndPoors"))
                    {
                        sLine +=
                            "\"" + dr["SectorCode"].ToString() + "\"," + "\"" + dr["SectorDesc"].ToString() + "\"," +
                            "\"" + dr["IndustryGroupCode"].ToString() + "\"," + "\"" + dr["IndustryGroupDesc"].ToString() + "\"," +
                            "\"" + dr["IndustryCode"].ToString() + "\"," + "\"" + dr["IndustryDesc"].ToString() + "\"," +
                            "\"" + dr["SubIndustryCode"].ToString() + "\"," + "\"" + dr["SubIndustryDesc"].ToString() + "\"";

                    }
                    else if (sVendor.Equals("Russell"))
                    {
                        sLine +=
                            "\"" + dr["SectorCode"].ToString() + "\"," + "\"" + dr["SectorDesc"].ToString() + "\"," +
                            "\"" + dr["SubSectorCode"].ToString() + "\"," + "\"" + dr["SubSectorDesc"].ToString() + "\"," +
                            "\"" + dr["IndustryCode"].ToString() + "\"," + "\"" + dr["IndustryDesc"].ToString() + "\"";
                    }
                    logHelper.WriteLine(sLine);
                }
                dr.Close();

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
        }


        #endregion End Security Master

        #region Total Return
        public void AddTotalReturn(string sDate, string sIndexName, string sVendorFormat,
                                   double dReturn, string sWhichReturn)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionString);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from TotalReturns
                    where IndexName = @IndexName 
                    and ReturnDate = @ReturnDate 
                    and VendorFormat = @VendorFormat
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@VendorFormat", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@ReturnDate"].Value = oDate;
                cmd.Parameters["@VendorFormat"].Value = sVendorFormat;
                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText =
                        "insert into TotalReturns (IndexName, ReturnDate, VendorFormat, " + sWhichReturn + ") " +
                        "Values (@IndexName, @ReturnDate, @VendorFormat, @" + sWhichReturn + ")";
                }
                else
                {
                    cmd.CommandText =
                        "update TotalReturns set " + sWhichReturn + " = @" + sWhichReturn + " " +
                        "where IndexName = @IndexName and ReturnDate = @ReturnDate and VendorFormat = @VendorFormat";
                }
                cmd.Parameters.Add("@" + sWhichReturn, SqlDbType.Float, 8);
                cmd.Parameters["@" + sWhichReturn].Value = dReturn;
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }

        }
        #endregion End Total Return

        /***************************************************/
        #endregion  End Production Code used by processor.process2()


        #region Testing Code used by Form2.Russell Tab
        /***************************************************/

        /*
        <?xml version="1.0"?>
        <AdventXML version="3.0">
        <AccountProvider name="Russell" code="rl">
        <XSXList index="rmidg" date="20110502" batch="11">
        <XSXPeriod from="20110429" through="20110430" indexperfiso="usd">
        <XSXDetail type="cs" iso="usd" symbol="a" weight="0.8377" irr="0"/>
        */
        // Constants to help parse above Advent(AXML)
        private const string AXML_LIST_TAG_XSX = "<XSXList";
        private const string AXML_PERIOD_BEGIN_TAG_XSX = "<XSXPeriod";
        private const string AXML_PERIOD_END_TAG_XSX = "</XSXPeriod";
        private const string AXML_DETAIL_TAG_XSX = "<XSXDetail";

        private const string AXML_LIST_TAG_XNX = "<XNXList";
        private const string AXML_PERIOD_BEGIN_TAG_XNX = "<XNXPeriod";
        private const string AXML_PERIOD_END_TAG_XNX = "</XNXPeriod";
        private const string AXML_DETAIL_TAG_XNX = "<XNXDetail";

        public void AddRussellAxmlSectorData(string FileName, string Source)
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            DateTime oPeriodEndDate = DateTime.MinValue;
            string Identifier = "";
            string Indexname = "";
            string IRR = "";
            string Weight = "";
            StreamReader srAxmlFile = null;
            bool FoundIndexName = false;
            bool FoundPeriodEndDate = false;
            int AddCount = 0;
            int LineCount = 0;
            string VendorFormat = null;

            string[] Split = null;

            for (srAxmlFile = new StreamReader(FileName)
               ; srAxmlFile.EndOfStream == false
               ;)
            {
                TextLine = srAxmlFile.ReadLine();
                LineCount += 1;

                if (!FoundIndexName)
                {
                    FoundIndexName = TextLine.Contains(AXML_LIST_TAG_XNX);
                    if (FoundIndexName)
                    {
                        Split = TextLine.Split('\"');
                        Split = TextLine.Split('"');
                        Indexname = Split[1];
                    }
                }

                if (!FoundPeriodEndDate)
                {
                    FoundPeriodEndDate = TextLine.Contains(AXML_PERIOD_BEGIN_TAG_XNX);
                    if (FoundPeriodEndDate)
                    {
                        Split = TextLine.Split('\"');
                        string sDate = Split[3];
                        DateTime.TryParseExact(sDate, "yyyyMMdd", enUS, DateTimeStyles.None, out oPeriodEndDate);
                        VendorFormat = Split[7];
                    }
                }

                if (FoundIndexName && FoundPeriodEndDate && TextLine.Contains(AXML_DETAIL_TAG_XNX))
                {
                    Split = TextLine.Split('\"');
                    Identifier = Split[1];
                    Weight = Split[3];
                    IRR = Split[5];
                    AddRussellDailyOutput(Indexname, oPeriodEndDate, VendorFormat, Source, Identifier, Weight, IRR);
                    AddCount += 1;
                }

                if (FoundPeriodEndDate && TextLine.Contains(AXML_PERIOD_END_TAG_XNX))
                {
                    FoundPeriodEndDate = false;
                }

            }
            //swLogFile.Flush();
            srAxmlFile.Close();
        }


        public void AddRussellAxmlSecurityData(string FileName, string Source)
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            DateTime oPeriodEndDate = DateTime.MinValue;
            string Ticker = "";
            string Indexname = "";
            string IRR = "";
            string Weight = "";
            StreamReader srAxmlFile = null;
            bool FoundIndexName = false;
            bool FoundPeriodEndDate = false;
            int AddCount = 0;
            int LineCount = 0;

            string[] Split = null;

            for (srAxmlFile = new StreamReader(FileName)
               ; srAxmlFile.EndOfStream == false
               ;)
            {
                TextLine = srAxmlFile.ReadLine();
                LineCount += 1;

                if (!FoundIndexName)
                {
                    FoundIndexName = TextLine.Contains(AXML_LIST_TAG_XSX);
                    if (FoundIndexName)
                    {
                        Split = TextLine.Split('\"');
                        Split = TextLine.Split('"');
                        Indexname = Split[1];
                    }
                }

                if (!FoundPeriodEndDate)
                {
                    FoundPeriodEndDate = TextLine.Contains(AXML_PERIOD_BEGIN_TAG_XSX);
                    if (FoundPeriodEndDate)
                    {
                        Split = TextLine.Split('\"');
                        string sDate = Split[3];
                        DateTime.TryParseExact(sDate, "yyyyMMdd", enUS, DateTimeStyles.None, out oPeriodEndDate);
                    }
                }

                if (FoundIndexName && FoundPeriodEndDate && TextLine.Contains(AXML_DETAIL_TAG_XSX))
                {
                    Split = TextLine.Split('\"');
                    Ticker = Split[5];
                    Weight = Split[7];
                    IRR = Split[9];
                    AddRussellDailyOutput(Indexname, oPeriodEndDate, "RussellSecurity", Source, Ticker, Weight, IRR);
                    AddCount += 1;
                }
            }
            //swLogFile.Flush();
            srAxmlFile.Close();
        }

        private struct IndexReturnStruct
        {
            public double IndexReturn1;
            public double IndexReturn2;
            public string IndexDate;
        }

        public string[] GetIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;

            try
            {
                string SqlSelectCount = "select count(IndexClientName) from VendorIndexMap ";
                string SqlSelect = "select IndexClientName from VendorIndexMap ";
                string SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                string SqlOrderBy = "order by IndexClientName";

                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    Indices = new string[count];
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        Indices[i] = dr["IndexClientName"].ToString();
                        i += 1;
                    }
                }
            }
            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
            }

            return (Indices);
        }


        /*
         * JK 3/11/2011:
         * The following routine started generating the following error msg:
         * 
         * The CLR has been unable to transition from COM context 0x118b008 
         * to COM context 0x118b178 for 60 seconds. The thread that owns the 
         * destination contex/apartment is most likely either doing a non 
         * pumping wait or processing a very long running operation without 
         * pumping Windows messages. This situation generally has a negative 
         * performance impact and may even lead to the application becoming 
         * non responsive or memory usage accumulating continually over time. 
         * To avoid this problem, all single threaded apartment (STA) threads 
         * should use pumping wait primitives (such as CoWaitForMultipleHandles) 
         * and routinely pump messages during long running operations.
         * 
         * Resolution 
         * 
         * Follow COM rules regarding STA message pumping. 
         * 
         * To avoid these error popups from appearing, select Exceptions 
         * from the Debug menu from Visual Studio window and in the Exception 
         * Dialog box select the Managed Debugging Assistants Exception Node. 
         * Then select ContextSwitchDeadlock and remove the select from Thrown 
         * column.
         * 
         */

        public void CalculateReturnsForPeriod(string sStartDate, string sEndDate, string sIndexName)
        {
            SqlConnection conn = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            string sMsg = null;
            double dReturn = 0.0;
            IndexReturnStruct[] IndexReturnArray;
            try
            {
                // See notes above the routine if a runtime error is generated
                sMsg = "CalculateReturnsForPeriod: ";
                logHelper.WriteLine(sMsg + sStartDate + " to " + sEndDate + " index " + sIndexName);
                string SqlSelect;
                string SqlWhere;
                SqlSelect = "select count (distinct FileDate) from RussellDailyHoldings1 ";
                SqlWhere = "where FileDate >= '" + sStartDate + "' ";
                SqlWhere += "and FileDate <= '" + sEndDate + "' ";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    IndexReturnArray = new IndexReturnStruct[count];
                    SqlSelect = "select distinct FileDate from RussellDailyHoldings1 ";
                    cmd.CommandText = SqlSelect + SqlWhere;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        string sDate = dr["FileDate"].ToString();
                        //logHelper.WriteLine("Processing: " + sDate);
                        // Uncomment this line to Calculate Return from RussellDailyHoldings
                        dReturn = CalculateReturnForDate(sDate, sIndexName, true);
                        // Uncomment this line to Select the Advent Adjusted Return from TotalReturns
                        //dReturn = GetAdvAdjReturnForDate(sDate, sIndexName);
                        IndexReturnArray[i].IndexDate = sDate;
                        IndexReturnArray[i].IndexReturn1 = dReturn;
                        IndexReturnArray[i].IndexReturn2 = 1 + (dReturn / 100);
                        i += 1;
                    }
                    double dProduct = 1;
                    for (i = 0; i < IndexReturnArray.Length; i++)
                    {
                        dProduct *= IndexReturnArray[i].IndexReturn2;
                    }
                    //dProduct = dProduct;
                    double dRetForPeriod = (dProduct - 1) * 100;
                    logHelper.WriteLine("Return for period " + sStartDate + " to " + sEndDate + " for " + sIndexName + " = " + dRetForPeriod);
                }
            }

            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
                logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                //logHelper.Flush();
            }

            return;
        }

        public bool GenerateConstituentReturnsForDate(string sDate, string sIndexName)
        {
            return (GenerateReturnsForDate(sDate, sIndexName, OutputTypes.CURRENT_TICKER));
        }

        public bool GenerateSectorReturnsForDate(string sDate, string sIndexName)
        {
            return (GenerateReturnsForDate(sDate, sIndexName, OutputTypes.SECTOR));
        }

        public bool GenerateSubSectorReturnsForDate(string sDate, string sIndexName)
        {
            return (GenerateReturnsForDate(sDate, sIndexName, OutputTypes.SUBSECTOR));
        }

        public bool GenerateIndustryReturnsForDate(string sDate, string sIndexName)
        {
            return (GenerateReturnsForDate(sDate, sIndexName, OutputTypes.INDUSTRY));
        }

        private bool GenerateReturnsForDate(string sDate, string sIndexName, OutputTypes OutputType)
        {
            string sMsg = null;
            mSqlConn = new SqlConnection(mConnectionString);
            bool ReturnsGenerated = false;

            try
            {
                sMsg = "GenerateConstituentReturnsForDate: " + sDate + " Index: " + sIndexName;
                logHelper.WriteLine(sMsg + sDate);
                /*
DECLARE @FileDate datetime,
        @IndexName varchar(12)
SET @FileDate = CAST('01/04/2017' AS datetime)
SET @IndexName = 'r3000'

SELECT
  h1.FileDate,
  h2.IndexName,
  h1.CUSIP,
  LOWER(h1.Ticker) AS Ticker,
  h1.SecurityReturn,
  h1.Sector,
  h1.SubSector,
  h1.Industry,
  ROUND((((CAST(h1.MktValue AS float) *
  (CAST(h2.SharesNumerator AS float) / h1.SharesDenominator)) / (SELECT
    SUM(CAST(h1.MktValue AS float) * (CAST(h2.SharesNumerator AS float) / h1.SharesDenominator))
  FROM RussellDailyHoldings1 h1
  INNER JOIN dbo.RussellDailyHoldings2 h2
    ON h1.FileDate = h2.FileDate
    AND h1.CUSIP = h2.CUSIP
  WHERE h2.FileDate = @FileDate
  AND h2.IndexName = @IndexName
  AND h2.SharesNumerator > 0
  AND h1.SharesDenominator > 0)
  ) * 100), 12) AS Weight
FROM RussellDailyHoldings1 h1
INNER JOIN dbo.RussellDailyHoldings2 h2
  ON h1.FileDate = h2.FileDate
  AND h1.CUSIP = h2.CUSIP
WHERE h2.FileDate = @FileDate
AND h2.IndexName = @IndexName
AND h2.SharesNumerator > 0
AND h1.SharesDenominator > 0                 
                 */
                /*
                declare
                    @FileDate datetime,
                    @IndexName varchar(12)   
                    set @FileDate = cast('04/18/2005' as datetime)
                    set @IndexName = 'r2000'
                 */
                string SqlSelect = @"
                    SELECT h1.FileDate, h2.IndexName, h1.CUSIP, lower(h1.Ticker) as Ticker, h1.SecurityReturn, 
                        h1.Sector, h1.SubSector, h1.Industry,
                        ROUND((( (cast(h1.MktValue as float) * 
                                    (cast(h2.SharesNumerator as float)/h1.SharesDenominator))/
                        (SELECT     
                            sum( cast(h1.MktValue as float) * 
                                 (cast(h2.SharesNumerator as float)/h1.SharesDenominator))
                         FROM         RussellDailyHoldings1 h1 
                            inner join dbo.RussellDailyHoldings2 h2 on 
                            h1.FileDate = h2.FileDate and 
                            h1.CUSIP = h2.CUSIP
                         WHERE 
                            h2.FileDate = @FileDate and 
                            h2.IndexName = @IndexName and 
                            h2.SharesNumerator > 0 and h1.SharesDenominator > 0)
                        ) * 100 ),12) As Weight
                    FROM         RussellDailyHoldings1 h1 inner join
                          dbo.RussellDailyHoldings2 h2 on 
                          h1.FileDate = h2.FileDate and 
                          h1.CUSIP = h2.CUSIP
                    WHERE 
                        h2.FileDate = @FileDate and 
                        h2.IndexName = @IndexName and 
                        h2.SharesNumerator > 0 and 
                        h1.SharesDenominator > 0
                ";

                string SqlOrderBy = "";
                switch (OutputType)
                {
                    case OutputTypes.CURRENT_TICKER:
                        SqlOrderBy = @"
                        ORDER BY Ticker
                        ";
                        break;
                    case OutputTypes.ORIGINAL_TICKER:
                        SqlOrderBy = @"
                        ORDER BY Ticker
                        ";
                        break;
                    case OutputTypes.SECTOR:
                        SqlOrderBy = @"
                        ORDER BY h1.Sector
                        ";
                        break;
                    case OutputTypes.SUBSECTOR:
                        SqlOrderBy = @"
                        ORDER BY h1.SubSector
                        ";
                        break;
                    case OutputTypes.INDUSTRY:
                        SqlOrderBy = @"
                        ORDER BY h1.Industry
                        ";
                        break;
                    default:
                        break;
                }

                logHelper.WriteLine(SqlSelect);
                mSqlConn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlOrderBy, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@FileDate"].Value = oDate;

                mSqlDr = cmd.ExecuteReader();
                if (mSqlDr.HasRows)
                {
                    ReturnsGenerated = true;
                    mPrevId = "";
                    ConstituentCount = 0;
                }
            }

            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (ReturnsGenerated);
        }


        public bool GetNextConstituentReturn(out string sCusip, out string sTicker, out string sWeight, out string sSecurityReturn)
        {
            string sMsg = null;
            bool GetNext = false;
            sCusip = "";
            sTicker = "";
            sWeight = "";
            sSecurityReturn = "";

            try
            {
                if (mSqlDr.HasRows)
                {
                    if ((GetNext = mSqlDr.Read()) == true)
                    {
                        sCusip = mSqlDr["CUSIP"].ToString();
                        string sOriginalTicker = mSqlDr["Ticker"].ToString();
                        sWeight = mSqlDr["Weight"].ToString();
                        sSecurityReturn = mSqlDr["SecurityReturn"].ToString();
                        ConstituentCount += 1;
                        string sCmpCusip = null;
                        string sNewCusip = null;
                        bool Done = false;
                        for (sCmpCusip = sCusip, sNewCusip = ""; !Done;)
                        {
                            sNewCusip = GetNewCUSIP(sCmpCusip);
                            if (sCmpCusip.Equals(sNewCusip))
                                Done = true;
                            else
                                sCmpCusip = sNewCusip;
                        }
                        string smCusip = null;
                        if (sCusip.Equals(sNewCusip))
                            smCusip = sCusip;
                        else
                            smCusip = sNewCusip;

                        string smTicker = GetSecurityMasterTicker(smCusip);
                        if (smTicker.Length > 0)
                            sTicker = smTicker;
                        else
                            sTicker = sOriginalTicker;

                        sMsg = sTicker + "," + sWeight + "," + sSecurityReturn + "," + sCusip;
                        logHelper.WriteLine(sMsg);


                        if ((mPrevId.Length > 0) && sCusip.Equals(mPrevId))
                        {
                            sMsg = "GetNextConstituentReturn: duplicate, " + sCusip;
                            logHelper.WriteLine(sMsg);
                        }
                        mPrevId = sCusip;
                    }
                    else
                    {
                        sMsg = "GetNextConstituentReturn:," + ConstituentCount.ToString();
                        logHelper.WriteLine(sMsg);
                        if (ConstituentCount > 0)
                        {
                            CloseGlobals();
                        }
                    }
                }
            }

            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (GetNext);
        }

        public bool GetNextSectorReturn(out string sSector, out string sWeight, out string sReturn)
        {
            return (GetNextRgsReturn("Sector", out sSector, out sWeight, out sReturn));
        }

        public bool GetNextSubSectorReturn(out string sSubSector, out string sWeight, out string sReturn)
        {
            return (GetNextRgsReturn("SubSector", out sSubSector, out sWeight, out sReturn));
        }

        public bool GetNextIndustryReturn(out string sIndustry, out string sWeight, out string sReturn)
        {
            return (GetNextRgsReturn("Industry", out sIndustry, out sWeight, out sReturn));
        }

        public bool GetNextRgsReturn(string sRgsCategory, out string sRgsId, out string sWeight, out string sReturn)
        {
            bool GetNext = false;
            sRgsId = "";
            sWeight = "";
            sReturn = "";
            string sMsg = "";

            CultureInfo OutputCultureInfo = CultureInfo.GetCultureInfo("en-US");

            try
            {
                if (mSqlDr.HasRows)
                {
                    bool done = false;
                    while (!done)
                    {
                        if ((GetNext = mSqlDr.Read()) == true)
                        {
                            if (mPrevId.Length == 0)
                            {
                                mPrevId = mSqlDr[sRgsCategory].ToString();
                                mRolledUpWeight = Convert.ToDouble(mSqlDr["Weight"].ToString());
                                mRolledUpReturn = Convert.ToDouble(mSqlDr["SecurityReturn"].ToString());
                            }
                            else if (mPrevId.Equals(mSqlDr[sRgsCategory].ToString()))
                            {
                                mRolledUpWeight += Convert.ToDouble(mSqlDr["Weight"].ToString());
                                mRolledUpReturn += Convert.ToDouble(mSqlDr["SecurityReturn"].ToString());
                            }
                            else
                            {
                                done = true;
                                sRgsId = mPrevId;
                                sWeight = mRolledUpWeight.ToString(mNumberFormat12, OutputCultureInfo);
                                sReturn = mRolledUpReturn.ToString(mNumberFormat4, OutputCultureInfo);
                                mPrevId = mSqlDr[sRgsCategory].ToString();
                                mRolledUpWeight = Convert.ToDouble(mSqlDr["Weight"].ToString());
                                mRolledUpReturn = Convert.ToDouble(mSqlDr["SecurityReturn"].ToString());
                            }
                        }
                        else
                        {
                            done = true;
                            sRgsId = mPrevId;
                            sWeight = mRolledUpWeight.ToString(mNumberFormat12, OutputCultureInfo);
                            sReturn = mRolledUpReturn.ToString(mNumberFormat4, OutputCultureInfo);
                            CloseGlobals();
                        }
                    }
                    sMsg = sRgsId + "," + sWeight + "," + sReturn;
                    logHelper.WriteLine(sMsg);
                }
            }


            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (GetNext);
        }



        private double CalculateReturnForDate(string sDate, string sIndexName, bool bSaveReturnInDb)
        {
            SqlConnection conn = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            double dReturn = 0.0;
            // string sMsg = null;

            try
            {
                //sMsg = "CalculateReturnForDate: ";
                //logHelper.WriteLine(sMsg + sDate );
                string SqlSelect = @"
                    Select SUM(WeightedCalcReturn9) FROM ( 
                    SELECT ROUND((( (cast(h1.MktValue as float) * 
                                    (cast(h2.SharesNumerator as float)/h1.SharesDenominator))/
                        (SELECT     
                            sum( cast(h1.MktValue as float) * 
                                 (cast(h2.SharesNumerator as float)/h1.SharesDenominator))
                         FROM         RussellDailyHoldings1 h1 
                            inner join dbo.RussellDailyHoldings2 h2 on 
                            h1.FileDate = h2.FileDate and 
                            h1.CUSIP = h2.CUSIP
                         WHERE 
                            h2.FileDate = @FileDate and 
                            h2.IndexName = @IndexName and 
                            h2.SharesNumerator > 0 and h1.SharesDenominator > 0)
                        ) * cast(h1.SecurityReturn as float) ),9) As WeightedCalcReturn9 
                    FROM         RussellDailyHoldings1 h1 inner join
                          dbo.RussellDailyHoldings2 h2 on 
                          h1.FileDate = h2.FileDate and 
                          h1.CUSIP = h2.CUSIP
                    WHERE 
                        h2.FileDate = @FileDate and 
                        h2.IndexName = @IndexName and 
                        h2.SharesNumerator > 0 and 
                        h1.SharesDenominator > 0
                    ) as SumWeightedCalcReturn9 
                ";

                //logHelper.WriteLine(SqlSelect);
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect, conn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@FileDate"].Value = oDate;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        dReturn = (double)dr[0];
                        dReturn = Math.Round(dReturn, 9, MidpointRounding.AwayFromZero);
                        string sReturn = dReturn.ToString();
                        
                        logHelper.WriteLine(sDate + " " + sReturn);
                        if (bSaveReturnInDb)
                        {
                            AddTotalReturn(sDate, sIndexName, (string)"RussellSecurity", dReturn, "AdvReturnDb");
                        }
                    }
                }
            }

            catch (SqlException ex)
            {
                logHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
                //swLogFile.Flush();
                //logHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (dReturn);
        }



        public string GetNewCUSIP(string sOldCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            string sNewCUSIP = sOldCUSIP;

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT newSymbol FROM HistoricalCusipChanges WHERE oldSymbol = @oldSymbol
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@oldSymbol", SqlDbType.VarChar, 8);
                cmd.Parameters["@oldSymbol"].Value = sOldCUSIP;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sNewCUSIP = dr["newSymbol"].ToString();
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }

            return (sNewCUSIP);
        }

        public string GetSecurityMasterTicker(string sCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            SqlDataReader dr = null;
            string sTicker = "";

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT Ticker FROM HistoricalSecurityMaster WHERE Cusip = @Cusip
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar, 8);
                cmd.Parameters["@Cusip"].Value = sCUSIP;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sTicker = dr["Ticker"].ToString();
                    }
                }

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }

            return (sTicker);
        }



        public void AddCalculatedTotalReturn(string sDate, string sIndexName, double dReturn)
        {
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    select count(*) from TotalReturns
                    where IndexName = @IndexName and ReturnDate = @ReturnDate 
                    and VendorFormat = 'RussellSecurity'
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@ReturnDate"].Value = oDate;
                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText = @"
                        insert into TotalReturns
                        (IndexName, ReturnDate, VendorFormat AdvReturnDb) Values 
                        (@IndexName, @ReturnDate, 'RussellSecurity', @AdvReturnDb)
                        ";
                }
                else
                {
                    cmd.CommandText = @"
                        update TotalReturns set AdvReturnDb = @AdvReturnDb
                        where IndexName = @IndexName and ReturnDate = @ReturnDate 
                        and VendorFormat = 'RussellSecurity'
                        ";
                }
                cmd.Parameters.Add("@AdvReturnDb", SqlDbType.Float, 8);
                cmd.Parameters["@AdvReturnDb"].Value = dReturn;
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                cnSql.Close();
            }

        }

        public double GetAdvAdjReturnForDate(string sDate, string sIndexName)
        {
            SqlDataReader dr = null;
            SqlConnection cnSql = new SqlConnection(mConnectionString);
            double dReturn = 0.0;
            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    select AdvReturnAdj from TotalReturns
                    where IndexName = @IndexName and ReturnDate = @ReturnDate 
                    and VendorFormat = 'RussellSecurity'
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@ReturnDate"].Value = oDate;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        dReturn = (double)dr[0];
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
                cnSql.Close();
            }
            return (dReturn);
        }


        public void AddRussellDailyOutput(
            string IndexName,
            DateTime ReturnDate,
            string VendorFormat,
            string Source,
            string Identifier,
            string Weight,
            string IRR
            )
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionString);
                    mSqlConn.Open();
                }

                string SqlSelect = @"
                    select count(*) from RussellDailyOutput where 
                    IndexName = @IndexName and 
                    ReturnDate = @ReturnDate and 
                    VendorFormat = @VendorFormat and
                    Source = @Source and
                    Identifier = @Identifier                    
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@VendorFormat", SqlDbType.VarChar);
                cmd.Parameters.Add("@Source", SqlDbType.VarChar);
                cmd.Parameters.Add("@Identifier", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = IndexName;
                cmd.Parameters["@ReturnDate"].Value = ReturnDate;
                cmd.Parameters["@VendorFormat"].Value = VendorFormat;
                cmd.Parameters["@Source"].Value = Source;
                cmd.Parameters["@Identifier"].Value = Identifier;

                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText = @"
                        insert into RussellDailyOutput
                        (IndexName, ReturnDate, VendorFormat, Source, Identifier, Weight, IRR) Values
                        (@IndexName, @ReturnDate, @VendorFormat, @Source, @Identifier, @Weight, @IRR)
                        ";
                }

                cmd.Parameters.Add("@Weight", SqlDbType.VarChar);
                cmd.Parameters.Add("@IRR", SqlDbType.VarChar);
                cmd.Parameters["@Weight"].Value = Weight;
                cmd.Parameters["@IRR"].Value = IRR;
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
        }
        /***************************************************/
        #endregion Testing Code used by Form2.Russell Tab

    }
}
