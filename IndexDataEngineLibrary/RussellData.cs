using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;

using AdventUtilityLibrary;


namespace IndexDataEngineLibrary
{
    public sealed class RussellData
    {
        //private DateHelper dateHelper;

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

        private CultureInfo mCultureInfo = new CultureInfo("en-US");

        private List<IndexRow> indexRowsTickerSort = new List<IndexRow>();
        private List<IndexRow> indexRowsIndustrySort = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel1RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel2RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel3RollUp = new List<IndexRow>();

        private const string NumberFormat = "0.#########";        

        private SharedData sharedData = null;

        private enum VendorFileFormats
        {
            H_OPEN_RGS,
            H_CLOSE_RGS,
            ALL
        }

        private int[] VendorFileRecLengths = new int[]
        {
            605,    //  H_OPEN_RGS
            591,    //  H_CLOSE_RGS
            153    //  ALL
        };

        /*
                public enum VendorFormats
                {
                    RUSSELLSECURITY,
                    RUSSELLSECTOR,
                    RUSSELLSUBSECTOR,
                    RUSSELLINDUSTRY,
                }
                */
        //private string[] sVendorFormats = new string[]
        //{
        //"RussellSecurity",
        //"RussellSector",
        //"RussellIndustry",
        //"RussellSubSector"
        //};

        private const string TOTAL_COUNT = "Total Count:";

        #endregion End privates, enums, constants

        #region Production Code used by processor.process2()
        /***************************************************/

        #region Constructor / Finish 
        //public RussellData()
        //{
        //    //private const string SQL_CONN = "server=JKERMOND\\JKERMOND;database=AdvIndexData;uid=sa;pwd=M@gichat!";
        //    // mConnectionString = ConfigurationManager.AppSettings["AdoConnectionString"];
        //    OpenLogFile();
        //}

        public RussellData()
        {
            //dateHelper = 
            //LogHelper.Info("RussellData()", "RussellData");
            sharedData = new SharedData();
            DateHelper.ConnectionString = sharedData.ConnectionStringAmdVifs;
            sharedData.Vendor = Vendors.Russell;
        }

        //public void SetConnectionString(string ConnectionString)
        //{
        //    mConnectionString = ConnectionString;
        //}


        private void OpenLogFile()
        {
            //private const string PATH = @"D:\IndexData\Russell\Test\";
            //private const string LOG_FILE = "RussellData.txt";
            LogFileName = AppSettings.Get<string>("RussellLogFile");
            //if (File.Exists(LogFileName))
            //    File.Delete(LogFileName);
            if (swLogFile == null)
            {
                if (!File.Exists(LogFileName))
                    swLogFile = File.CreateText(LogFileName);
                else
                    swLogFile = File.AppendText(LogFileName);

                //LogHelper.WriteLine("Russell Data started: " + DateTime.Now);
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

        #region ProcessVendorDatasetJobs

        public void ProcessVendorDatasetJobs(string Dataset, string sProcessDate)
        {

            try
            {
                DateTime ProcessDate = Convert.ToDateTime(sProcessDate);

                ProcessVendorFiles(ProcessDate, ProcessDate, Dataset, true, true, true, true, true);
                string IndexName = "";
                string[] Indices = null;
                Indices = GetIndices();
                for (int i = 0; i < Indices.Length; i++)
                {
                    IndexName = Indices[i];
                    if (ProcessStatus.GenerateReturns(sProcessDate, Vendors.Russell.ToString(), Dataset, IndexName))
                    {
                        GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Constituent, false);
                        ProcessStatus.Update(sProcessDate, Vendors.Russell.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlConstituentData, ProcessStatus.StatusValue.Pass);
                        GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Sector, false);
                        ProcessStatus.Update(sProcessDate, Vendors.Russell.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlSectorData, ProcessStatus.StatusValue.Pass);
                        sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.Russell, Dataset, IndexName, AdventOutputType.Constituent);
                        sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.Russell, Dataset, IndexName, AdventOutputType.Sector);
                    }
                }
                sharedData.VendorDatasetJobsUpdateProcessDate(Vendors.Russell.ToString(), Dataset, sProcessDate);
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: VendorDatasetJobs complete  " + Vendors.Russell.ToString() + " " + Dataset + " " + sProcessDate);
                IndexDataEngine indexDataEngine = new IndexDataEngine();
                indexDataEngine.GenerateSecurityMasterChangesDataAndReport(sProcessDate);
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }

        }

        #endregion

        #region Daily Holdings File Processing

        public bool HoldingsFilesUpdated(DateTime ProcessDate)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                    LogHelper.WriteLine(ex.Message);
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
                LogHelper.WriteLine("HoldingsFilesUpdated() == false");
                LogHelper.WriteLine("Process Date      : " + ProcessDate.ToShortDateString());
                LogHelper.WriteLine("Open File Date    : " + OpenFileDate.ToShortDateString());
                LogHelper.WriteLine("Close File Date   : " + CloseFileDate.ToShortDateString());
                LogHelper.WriteLine("Open Vendor Total : " + OpenVendorTotal.ToString());
                LogHelper.WriteLine("Open Advent Total : " + OpenAdventTotal.ToString());
                LogHelper.WriteLine("Close Vendor Total: " + CloseVendorTotal.ToString());
                LogHelper.WriteLine("Close Advent Total: " + CloseAdventTotal.ToString());
            }
            return (bUpdated);
        }

        public void ProcessVendorFiles(DateTime oStartDate, DateTime oEndDate, string Dataset, bool bOpenFiles, bool bCloseFiles, 
                                       bool bTotalReturnFiles, bool bSecurityMaster, bool bSymbolChanges)
        {
            DateTime oProcessDate;
            int DateCompare;
            string FilePath = AppSettings.Get<string>("VifsPath.Russell");
            string FileName;
            string sMsg = "ProcessVendorFiles: ";

            LogHelper.WriteLine(sMsg + "Started " + DateTime.Now);

            try
            {
                for (oProcessDate = oStartDate
                   ; (DateCompare = oProcessDate.CompareTo(oEndDate)) <= 0
                   ; oProcessDate = oProcessDate.AddDays(1))
                {
                    //DateHelper.IsHoliday(oProcessDate);                    
                    if (bOpenFiles || bSymbolChanges)
                    {
                        FileName = FilePath + "H_OPEN_R3000E_" + oProcessDate.ToString("yyyyMMdd") + "_RGS.TXT";
                        if (File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            if (bOpenFiles)
                            {
                                AddRussellOpeningData(VendorFileFormats.H_OPEN_RGS, FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Russell.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                                ProcessStatus.Update(oProcessDate, Vendors.Russell.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);

                            }
                            if (bSymbolChanges)
                            {
                                AddRussellSymbolChangeData(VendorFileFormats.H_OPEN_RGS, FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Russell.ToString(), Dataset, "", ProcessStatus.WhichStatus.SymbolChangeData, ProcessStatus.StatusValue.Pass);
                            }
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }

                    if (bCloseFiles)
                    {
                        FileName = FilePath + "H_" + oProcessDate.ToString("yyyyMMdd") + "_RGS_R3000E.TXT";
                        if (File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            AddRussellClosingData(VendorFileFormats.H_CLOSE_RGS, FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Russell.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }
                    if (bTotalReturnFiles)
                    {
                        FileName = FilePath + "ALL" + oProcessDate.ToString("yyyyMMdd") + ".TXT";
                        if (File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            AddRussellTotalReturnData(VendorFileFormats.ALL, FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Russell.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }                    
                }
            }
            catch
            {
            }
            finally
            {
                LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                RussellData_Finish();
            }
        }

        private string GetField(string TextLine, int Start, int Length)
        {
            string sFld = TextLine.Substring(Start - 1, Length);
            sFld = sFld.Trim();
            return (sFld);
        }

        private void AddTotalConstituentCounts(
            DateTime FileDate, VendorFileFormats FileFormat, string FileName,
            string TextLine, int SecuritiesTotal, int ZeroSharesTotal)
        {
            int Pos = TextLine.IndexOf(TOTAL_COUNT);
            int VendorTotal = Convert.ToInt32(TextLine.Substring(Pos + TOTAL_COUNT.Length));
            int AdventTotal = SecuritiesTotal - ZeroSharesTotal;
            LogHelper.WriteLine(FileName + ": SecurityCount " + AdventTotal +
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

            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            try
            {
                string sFileType = "";
                switch (FileFormat)
                {
                    case VendorFileFormats.H_CLOSE_RGS:
                        sFileType = "Close";
                        break;
                    case VendorFileFormats.H_OPEN_RGS:
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
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                cnSql.Close();
            }



            return;
        }

        internal const double NullDouble = double.MinValue;

        internal double StringToDouble(string number)
        {
            // This needs to go first since it is fairly common.
            if (string.IsNullOrEmpty(number))
                return 0;

            //if (string.IsNullOrEmpty(number.Replace(".", "").Trim())) 
            //    return 0;

            try
            {
                return Convert.ToDouble(number, mCultureInfo);
            }
            catch
            {
                // Some special formats.
                if (number.Equals("#N/A", StringComparison.OrdinalIgnoreCase) || number == "-" || number.StartsWith("..."))
                    return 0;
                else if (number.StartsWith("(", StringComparison.Ordinal) &&
                    number.EndsWith(")", StringComparison.Ordinal) && 3 <= number.Length)
                {
                    try { return Convert.ToDouble("-" + number.Substring(1, number.Length - 2), mCultureInfo); }
                    catch { return NullDouble; }
                }
                else
                    return NullDouble;
            }
        }

        private void AddRussellSymbolChangeData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {

            string[] sIndices = GetVendorIndices();
            //foreach (string sIndex in sIndices)
            //{
            //    DeleteTotalReturn(FileDate, sIndex);
            //}

            bool bTotalCount = false;
            bool bHeader1 = false;
            bool bHeader2 = false;
            StreamReader srFile = null;

            for (srFile = new StreamReader(FileName)
               ; srFile.EndOfStream == false
               ;)
            {
                string sDate = "";
                DateTime oDate = DateTime.MinValue;
                string sOldSymbol = "";
                string sNewSymbol = "";
                string sCompanyName = "";
                string TextLine = srFile.ReadLine();
                /*
                Total Count:                                3526                 
                Date   Old Identifier    New Identifier         Name
                -------- --------------    --------------   -------------------
20170103 831756101         02874P103        AMERICAN OUTDOOR BRANDS
                20170103 SWHC              AOBC             AMERICAN OUTDOOR BRANDS
                20170103 385002100         385002308        GRAMERCY PROPERTY TRUST
                20170103 502424104         502413107        L3 TECHNOLOGIES
                20170103 26168L205         50189K103        LCI INDUSTRIES
                20170103 DW                LCII             LCI INDUSTRIES
                20170103 761283100         74967X103        RH
                */
                if (!bTotalCount)
                    bTotalCount = TextLine.Contains("Total Count:");
                if (bTotalCount && !bHeader1)
                    bHeader1 = (TextLine.StartsWith("  Date") == true);
                if(bTotalCount && bHeader1 && !bHeader2)
                    bHeader2 = (TextLine.StartsWith("------") == true);
                if(bTotalCount && bHeader1 && bHeader2 && TextLine.StartsWith("20"))
                {
                    sDate = GetField(TextLine, 1, 8);
                    if (sDate.Length == 8)
                        DateTime.TryParseExact(sDate, "yyyyMMdd", mCultureInfo, DateTimeStyles.None, out oDate);
                    sOldSymbol = GetField(TextLine, 10, 8);
                    sNewSymbol = GetField(TextLine, 28, 8);
                    sCompanyName = GetField(TextLine, 45, TextLine.Length - 45 + 1 );
                    sharedData.AddSymbolChange( "R", oDate, sOldSymbol, sNewSymbol, sCompanyName);
                }
            }
            srFile.Close();
        }


        private void AddRussellTotalReturnData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {

            string[] sIndices = GetVendorIndices();
            //foreach (string sIndex in sIndices)
            //{
            //    DeleteTotalReturn(FileDate, sIndex);
            //}

            StreamReader srFile = null;
            for ( srFile = new StreamReader(FileName)
               ; srFile.EndOfStream == false
               ;)
            {
                bool IsHeader = false;
                string sVendorIndex = "";
                string sAdventIndex = "";
                string sCurrency = "";
                string sTotal = "";
                string TextLine = srFile.ReadLine();
                /*

INDEX     DATE       CRNY  TOTAL        PRICE       100% HEDGED      NET              BAMV                    EMV                    CASH DIV       COUNT
RU3000    20170103   USD   6792.92649   2466.56602   1972.19108   1860.35653      23369771298.25550      23562268983.97530           597209.90000    2978
RU3000    20170103   AUD   2206.69607   1812.10850   2256.57456   2081.56375      32274231863.63735      32634721555.08207           827160.52557    2978
RU3000    20170103   CAD   2292.89757   1882.89599   1820.72665   2162.87713      31340031799.52560      31602893274.75688           801007.77837    2978
RU3000    20170103   CHF   1662.25918   1365.02441   1696.48181   1567.99955      23751867058.98201      24244396671.06139           614499.12660    2978

                */

                if (TextLine.Length == VendorFileRecLengths[(int)FileFormat])
                {
                    IsHeader = (TextLine.StartsWith("INDEX") == true);
                    if (!IsHeader && TextLine.StartsWith("R"))
                    {
                        sCurrency = GetField(TextLine, 22, 3);

                        if (TextLine.StartsWith("R") && !IsHeader && sCurrency.Equals("USD"))
                        {
                            sVendorIndex = GetField(TextLine, 1, 10);
                            sCurrency = GetField(TextLine, 22, 3);
                            sTotal = GetField(TextLine, 28, 10);
                            if( sIndices.Contains(sVendorIndex))
                            {
                                sAdventIndex = GetAdventIndex(sVendorIndex);
                                DeleteRussellTotalReturnForIndex(FileDate, sAdventIndex);
                                AddRussellTotalReturnForIndex(FileDate, sAdventIndex, sTotal);
                                string sStartAndEndDate = FileDate.ToString("MM/dd/yyyy");
                                CalculateVendorTotalReturnsForPeriod(sStartAndEndDate, sStartAndEndDate, sAdventIndex);
                                CalculateAdventTotalReturnsForPeriod(sStartAndEndDate, sStartAndEndDate, sAdventIndex);
                                CalculateAdjustedTotalReturnsForPeriod(sStartAndEndDate, sStartAndEndDate, sAdventIndex);

                            }
                        }
                    }
                }
            }
            srFile.Close();
        }

        private void AddRussellOpeningData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {
            SqlConnection cnSql1 = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlConnection cnSql2 = new SqlConnection(sharedData.ConnectionStringIndexData);
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmdHoldings = null;
            string Fld;
            DateTime oDate = DateTime.MinValue;
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

                if (TextLine.Length == VendorFileRecLengths[(int)FileFormat] && !FoundTotalCount)
                {
                    IsHeader1 = (TextLine.StartsWith("Date") == true);
                    IsHeader2 = (TextLine.StartsWith("----") == true);
                    bool ok = (TextLine.StartsWith("20") == true) && !IsHeader1 && !IsHeader2;

                    string sCUSIP = "";
                    string sTicker = "";
                    string sCompanyName = "";
                    string sSector = "";

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
                        { 
                            drHoldings1["CUSIP"] = Fld;
                            sCUSIP = Fld;
                        }
                        else
                            ok = false;

                        Fld = GetField(TextLine, 33, 7);
                        if (Fld.Length >= 1)
                        {
                            drHoldings1["Ticker"] = Fld;
                            sTicker = Fld;
                        }
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

                        Fld = GetField(TextLine, 164, 25);
                        sCompanyName = Fld;

                        Fld = GetField(TextLine, 278, 7);
                        drHoldings1["Sector"] = Fld;
                        sSector = Fld;
                    }

                    if (ok)
                    {
                    sharedData.AddSecurityMasterFull(sTicker, sCUSIP, "R", sCompanyName, sSector, oDate);

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
                                LogHelper.WriteLine(ex.Message);
                                LogHelper.WriteLine(FileName + ":" + TextLine);
                            }
                        }
                        finally
                        {
                        }
                    }
                }
                else if (FoundTotalCount)
                {
                    AddTotalConstituentCounts(FileDate, FileFormat, FileName, TextLine, SecurityCount, SharesDenominatorZeroCount);
                    break;
                }
                if (!add && !IsHeader1 && !IsHeader2 && (TextLine.Length > 0))
                {
                    LogHelper.WriteLine("Skipping line:" + TextLine.ToString());
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
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
            srHoldingsFile.Close();
        }



        private void AddRussellClosingData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
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

                if (TextLine.Length == VendorFileRecLengths[(int)FileFormat] && !FoundTotalCount)
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
                                        LogHelper.WriteLine("Skipping line:" + TextLine.ToString());
                                        LogHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString() +
                                                            " linked to Old CUSIP " + sOldCUSIP.ToString());
                                        ok = false;
                                    }

                                    //throw new Exception("AddRussellClosingData can't update " + sCUSIP + " for date" + oDate.ToShortDateString());
                                    else
                                        sCUSIP = sOldCUSIP;
                                }
                                else if (sOldCUSIP.Length == 0)
                                {
                                    LogHelper.WriteLine("Skipping line:" + TextLine.ToString());
                                    LogHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString());
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
                                LogHelper.WriteLine(ex.Message);
                                LogHelper.WriteLine(FileName + ":" + TextLine);
                            }
                        }
                        finally
                        {
                        }
                    }
                }
                else if (FoundTotalCount)
                {
                    AddTotalConstituentCounts(FileDate, FileFormat, FileName, TextLine, SecurityCount, SharesDenominatorZeroCount);
                    break;
                }
                if (!add && !IsHeader1 && !IsHeader2 && (TextLine.Length > 0))
                {
                    LogHelper.WriteLine("Skipping line:" + TextLine.ToString());
                }
            }
            srHoldingsFile.Close();
        }

        public int GetDailyHoldings1Count(DateTime FileDate, string sCUSIP)
        {
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                conn.Close();
            }

            return (Count);
        }


        public string GetOldCUSIP(string sNewCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                    LogHelper.WriteLine(ex.Message);
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
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                    LogHelper.WriteLine(ex.Message);
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

        public void GenerateHistSecMasterLists(string sListDate)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                    LogHelper.WriteLine(ex.Message);
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
                SqlConnection SqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
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
                    LogHelper.WriteLine(sLine);
                }
                dr.Close();

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
        }


        #endregion End Security Master

        #region Total Return
        public void AddRussellTotalReturnForIndex(DateTime oDate, string sIndexName, string sTotalReturn)
        {
            /*
            CREATE TABLE[dbo].[RussellDailyIndexReturns] (
	        [IndexName] [varchar] (9) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
            [FileDate] [DateOnly_Type] NOT NULL,
            [TotalReturn] [varchar] (12) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL 
            ) ON[PRIMARY]
            */
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from RussellDailyIndexReturns
                    where IndexName = @IndexName 
                    and FileDate = @FileDate 
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@TotalReturn", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                cmd.Parameters["@FileDate"].Value = oDate;
                cmd.Parameters["@TotalReturn"].Value = sTotalReturn;

                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText =
                        "insert into RussellDailyIndexReturns (IndexName, FileDate, TotalReturn) " +
                        "Values (@IndexName, @FileDate, @TotalReturn)";
                }
                //else
                //{
                //    cmd.CommandText =
                //        "update TotalReturns set " + sWhichReturn + " = @" + sWhichReturn + " " +
                //        "where IndexName = @IndexName and FileDate = @FileDate and VendorFormat = @VendorFormat";
                //}
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
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


        public string GetAdventIndex(string VendorIndex)
        {
            string AdventIndex = "";
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                //string SqlSelect = "select AdventIndexName from VendorIndexMap ";
                string SqlSelect = "select IndexClientName from VendorIndexMap ";
                string SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' and IndexName ='" + VendorIndex + "'";
//                string SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' and VendorIndexName ='" + VendorIndex + "'";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere, conn);
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
                    //AdventIndex = dr["AdventIndexName"].ToString();
                    AdventIndex = dr["IndexClientName"].ToString();
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
            }

            return (AdventIndex);
        }

        public string[] GetIndices()
        {
            sharedData.Vendor = Vendors.Russell;
            return (sharedData.GetIndices());
        }

        public string[] GetVendorIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                //string SqlSelectCount = "select count(VendorIndexName) from VendorIndexMap ";
                string SqlSelectCount = "select count(IndexName) from VendorIndexMap ";
                //string SqlSelect = "select VendorIndexName from VendorIndexMap ";
                string SqlSelect = "select IndexName from VendorIndexMap ";
                string SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                //string SqlOrderBy = "order by VendorIndexName";
                string SqlOrderBy = "order by IndexName";

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
                        //Indices[i] = dr["VendorIndexName"].ToString();
                        Indices[i] = dr["IndexName"].ToString();
                        i += 1;
                    }
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
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

        // CalculateAdventTotalReturnsForPeriod for Vendor Russell Advent calculation
        public void CalculateAdventTotalReturnsForPeriod(string sStartDate, string sEndDate, string sIndexName)
        {
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;
            string sMsg = null;
            double dReturn = 0.0;
            IndexReturnStruct[] IndexReturnArray;
            try
            {
                // See notes above the routine if a runtime error is generated
                sMsg = "CalculateAdventTotalReturnsForPeriod: ";
                //LogHelper.WriteLine(sMsg + sStartDate + " to " + sEndDate + " index " + sIndexName);
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
                        //LogHelper.WriteLine("Processing: " + sDate);
                        // Uncomment this line to Calculate Return from RussellDailyHoldings
                        dReturn = CalculateAdventTotalReturnForDate(sDate, sIndexName, true);
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
                    //LogHelper.WriteLine("Return for period " + sStartDate + " to " + sEndDate + " for " + sIndexName + " = " + dRetForPeriod);
                }
            }

            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
                LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                //LogHelper.Flush();
            }

            return;
        }

        private void AdjustReturnsToMatchPublishedTotalReturns(List<IndexRow> indexRows, string sDate, string sIndexName, string sVendorFormat)
        {
            int totalReturnPrecision = 9;
            double dZero = 0.0;

            double VendorTotalReturn = GetVendorTotalReturnForDate(sDate, sIndexName);

            if (VendorTotalReturn.Equals(dZero) == false)
            {

                VendorTotalReturn = Math.Round(VendorTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);

                IndexRows.ZeroAdventTotalReturn();
                foreach (IndexRow indexRow in indexRows)
                    indexRow.CalculateAdventTotalReturn();

                double AdventTotalReturn = IndexRows.AdventTotalReturn;
                AdventTotalReturn = Math.Round(AdventTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);

                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.Russell.ToString(), sVendorFormat, AdventTotalReturn, "AdvReturn");


                double AdventVsVendorDiff = VendorTotalReturn - AdventTotalReturn;

                AdventVsVendorDiff = Math.Round(AdventVsVendorDiff, totalReturnPrecision, MidpointRounding.AwayFromZero);

                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.Russell.ToString(), sVendorFormat, AdventVsVendorDiff, "Diff");


                IndexRows.CalculateAddlContribution(AdventVsVendorDiff, sVendorFormat);

                foreach (IndexRow indexRow in indexRows)
                {
                    indexRow.CalculateAdventAdjustedReturn();
                }


                double AdventTotalReturnAdjusted = IndexRows.AdventTotalReturnAdjusted;
                AdventTotalReturnAdjusted = Math.Round(AdventTotalReturnAdjusted, totalReturnPrecision, MidpointRounding.AwayFromZero);
                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.Russell.ToString(), sVendorFormat, AdventTotalReturnAdjusted, "AdvReturnAdj");
            }

        }

        private void RollUpRatesOfReturn(List<IndexRow> indexRowsRollUp, List<IndexRow> indexRowsIndustrySort, 
            IndexRow.VendorFormat vendorFormat, string sDate, string sIndexName)
        {
            foreach (IndexRow indexRowRollUp in indexRowsRollUp)
                foreach (IndexRow indexRowConstituent in indexRowsIndustrySort)
                {
                    string CompareIndentifier = "";
                    switch (vendorFormat)
                    {
                        case IndexRow.VendorFormat.SECTOR_LEVEL1:
                            CompareIndentifier = indexRowConstituent.SectorLevel1; break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL2:
                            CompareIndentifier = indexRowConstituent.SectorLevel2; break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL3:
                            CompareIndentifier = indexRowConstituent.SectorLevel3; break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL4:
                            CompareIndentifier = indexRowConstituent.SectorLevel4; break;
                    }
                    if (CompareIndentifier == indexRowRollUp.Identifier)
                        indexRowRollUp.RateOfReturn += indexRowConstituent.RateOfReturn * indexRowConstituent.Weight / indexRowRollUp.Weight;
                }

            //LogHelper.WriteLine("---Before---");
            //foreach (IndexRow indexRowRollUp in indexRowsRollUp)
            //{
            //    LogHelper.WriteLine(indexRowRollUp.Identifier + " " + indexRowRollUp.Weight.ToString() + " " + indexRowRollUp.RateOfReturn.ToString());
            //}

            AdjustReturnsToMatchPublishedTotalReturns(indexRowsRollUp, sDate, sIndexName, vendorFormat.ToString());
            //LogHelper.WriteLine("---After---");

            //foreach (IndexRow indexRowRollUp in indexRowsRollUp)
            //{
            //    LogHelper.WriteLine(indexRowRollUp.Identifier + " " + indexRowRollUp.Weight.ToString() + " " + indexRowRollUp.RateOfReturnAdjusted.ToString());
            //}
            //LogHelper.WriteLine("---Done---");
        }

        private void InitializeIndexRows()
        {
            IndexRows.Reset();
            indexRowsTickerSort.Clear();
            indexRowsIndustrySort.Clear();
            indexRowsSectorLevel1RollUp.Clear();
            indexRowsSectorLevel2RollUp.Clear();
            indexRowsSectorLevel3RollUp.Clear();
        }


        public void GenerateIndustryReturnsForDate(string sDate, string sIndexName)
        {
            InitializeIndexRows();
            if (GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Sector) == true)
            {
                for (bool GotNext = true; GotNext;)
                {
                    string sCusip = null;
                    string sTicker = null;
                    string sSector = null;
                    string sSubSector = null;
                    string sIndustry = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if ((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out sSector, out sSubSector, out sIndustry,
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker,
                                                         sSector, sSubSector, sIndustry, "",
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);
                        indexRowsIndustrySort.Add(indexRow);
                    }
                }

                
                for (IndexRow.VendorFormat vendorFormat = IndexRow.VendorFormat.SECTOR_LEVEL1; vendorFormat <= IndexRow.VendorFormat.SECTOR_LEVEL3; vendorFormat++)
                {
                    string sCurrentIdentifier = "";
                    double rolledUpWeight = 0;
                    foreach (IndexRow indexRow in indexRowsIndustrySort)
                    {
                        if (sCurrentIdentifier.Length == 0)
                        {
                            rolledUpWeight = indexRow.Weight;
                            sCurrentIdentifier = indexRow.GetIdentifier(vendorFormat);
                        }
                        else if (indexRow.GetIdentifier(vendorFormat) == sCurrentIdentifier)
                        {
                            rolledUpWeight += indexRow.Weight;
                        }
                        else if (indexRow.GetIdentifier(vendorFormat) != sCurrentIdentifier)
                        {
                            // finish up and store the current sector's info
                            switch (vendorFormat)
                            {
                                case IndexRow.VendorFormat.SECTOR_LEVEL1:
                                    IndexRow rollUpIndexRow1 = new IndexRow(sDate, sIndexName, "", "",
                                                                     sCurrentIdentifier, "", "", "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel1RollUp.Add(rollUpIndexRow1); break;
                                case IndexRow.VendorFormat.SECTOR_LEVEL2:
                                    IndexRow rollUpIndexRow2 = new IndexRow(sDate, sIndexName, "", "",
                                                                      "", sCurrentIdentifier, "", "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel2RollUp.Add(rollUpIndexRow2); break;
                                case IndexRow.VendorFormat.SECTOR_LEVEL3:
                                    IndexRow rollUpIndexRow3 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", "", sCurrentIdentifier, "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel3RollUp.Add(rollUpIndexRow3); break;
                            }

                            //... and then move on to start accumulating the next sector's info
                            rolledUpWeight = indexRow.Weight;
                            sCurrentIdentifier = indexRow.GetIdentifier(vendorFormat);
                        }
                        if (indexRowsIndustrySort.Last().Equals(indexRow))
                        {
                            switch (vendorFormat)
                            {
                                case IndexRow.VendorFormat.SECTOR_LEVEL1:
                                    IndexRow rollUpIndexRow1 = new IndexRow(sDate, sIndexName, "", "",
                                                                     sCurrentIdentifier, "", "", "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel1RollUp.Add(rollUpIndexRow1); break;
                                case IndexRow.VendorFormat.SECTOR_LEVEL2:
                                    IndexRow rollUpIndexRow2 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", sCurrentIdentifier, "", "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel2RollUp.Add(rollUpIndexRow2); break;
                                case IndexRow.VendorFormat.SECTOR_LEVEL3:
                                    IndexRow rollUpIndexRow3 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", "", sCurrentIdentifier, "",
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel3RollUp.Add(rollUpIndexRow3); break;
                            }
                        }
                    }

                    switch (vendorFormat)
                    {
                        case IndexRow.VendorFormat.SECTOR_LEVEL1:
                            RollUpRatesOfReturn(indexRowsSectorLevel1RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL2:
                            RollUpRatesOfReturn(indexRowsSectorLevel2RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL3:
                            RollUpRatesOfReturn(indexRowsSectorLevel3RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            break;
                    }


                    /*
                    foreach (IndexRow indexRowSector in indexRowsSectorLevel1RollUp)
                        foreach (IndexRow indexRowConstituent in indexRowsIndustrySort)
                        {
                            string CompareIndentifier = "";
                            switch (vendorFormat)
                            {
                                case IndexRow.VendorFormat.SECTOR:
                                    CompareIndentifier = indexRowConstituent.Sector; break;
                                case IndexRow.VendorFormat.SUBSECTOR:
                                    CompareIndentifier = indexRowConstituent.SubSector; break;
                                case IndexRow.VendorFormat.INDUSTRY:
                                    CompareIndentifier = indexRowConstituent.Industry; break;
                            }
                            if (CompareIndentifier == indexRowSector.Identifier)
                                indexRowSector.RateOfReturn += indexRowConstituent.RateOfReturn * indexRowConstituent.Weight / indexRowSector.Weight;
                        }

                    LogHelper.WriteLine("---Before---");
                    foreach (IndexRow indexRowSector in indexRowsSectorLevel1RollUp)
                    {
                        LogHelper.WriteLine(indexRowSector.Identifier + " " + indexRowSector.Weight.ToString() + " " + indexRowSector.RateOfReturn.ToString());
                    }

                    AdjustReturnsToMatchPublishedTotalReturns(indexRowsSectorLevel1RollUp, sDate, sIndexName);
                    LogHelper.WriteLine("---After---");

                    foreach (IndexRow indexRowSector in indexRowsSectorLevel1RollUp)
                    {
                        LogHelper.WriteLine(indexRowSector.Identifier + " " + indexRowSector.Weight.ToString() + " " + indexRowSector.RateOfReturnAdjusted.ToString());
                    }
                    LogHelper.WriteLine("---Done---");
                    */
                }
            }
        }


        public void GenerateReturnsForDateRange(string sStartDate, string sEndDate, string sIndexName, AdventOutputType adventOutputType, bool isHistoricalAxmlFile)
        {
            DateTime startDate = Convert.ToDateTime(sStartDate);
            DateTime endDate = Convert.ToDateTime(sEndDate);
            DateTime processDate;
            DateTime fileDate;
            int DateCompare;

            bool isFirstDate = true;
            bool isLastDate = true; ;

            for (processDate = startDate
            ; (DateCompare = processDate.CompareTo(endDate)) <= 0
            ; processDate = DateHelper.NextBusinessDay(processDate))
            {
                if( isHistoricalAxmlFile)
                {
                    if (processDate.Equals(startDate))
                        isFirstDate = true;
                    else
                        isFirstDate = false;

                    if (processDate.Equals(endDate))
                        isLastDate = true;
                    else
                        isLastDate = false;
                    fileDate = endDate;
                }
                else
                {
                    isFirstDate = true;
                    isLastDate = true;
                    fileDate = processDate;
                }


                if (adventOutputType.Equals(AdventOutputType.Constituent))
                {
                    GenerateConstituentReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    sharedData.GenerateAxmlFileConstituents(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"),
                                                            sIndexName, Vendors.Russell, indexRowsTickerSort,
                                                            isFirstDate, isLastDate);
                }
                else if (adventOutputType.Equals(AdventOutputType.Sector))
                {
                    GenerateIndustryReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    sharedData.GenerateAxmlFileSectors(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"),
                                                       sIndexName, Vendors.Russell,
                                                       indexRowsSectorLevel1RollUp, indexRowsSectorLevel2RollUp, indexRowsSectorLevel3RollUp,
                                                       isFirstDate, isLastDate);
                }
            }
        }


        public void GenerateConstituentReturnsForDate(string sDate, string sIndexName)
        {
            int i = 0;
            InitializeIndexRows();
            if (GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Constituent) == true)
            {
                for (bool GotNext = true; GotNext;)
                {
                    i += 1;
                    string sCusip = null;
                    string sTicker = null;
                    string sSector = null;
                    string sSubSector = null;
                    string sIndustry = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if ((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out sSector, out sSubSector, out sIndustry, 
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker, 
                                                         sSector, sSubSector, sIndustry, "",
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);
                        indexRow.CurrentTicker = sharedData.GetSecurityMasterCurrentTickerRussell(sTicker, sCusip, sDate);
                        indexRowsTickerSort.Add(indexRow);
                    }
                }
                AdjustReturnsToMatchPublishedTotalReturns(indexRowsTickerSort, sDate, sIndexName, IndexRow.VendorFormat.CONSTITUENT.ToString());
            }
        }


        private bool GenerateReturnsForDate(string sDate, string sIndexName, AdventOutputType OutputType)
        {
            string sMsg = null;
            mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
            bool ReturnsGenerated = false;

            try
            {
                sMsg = "GenerateReturnsForDate: " + sDate + " Index: " + sIndexName;
                LogHelper.WriteLine(sMsg);
                /*
                declare
                    @FileDate datetime,
                    @IndexName varchar(12)   
                    set @FileDate = cast('04/18/2005' as datetime)
                    set @IndexName = 'r2000'
                    DECLARE @FileDate nvarchar(10);
                    DECLARE @IndexName nvarchar(10);
                    SET @FileDate = '01/04/2017';
                    SET @IndexName = 'r3000';

                 */
                string SqlSelect = @"
                    SELECT h1.FileDate, h2.IndexName, h1.CUSIP, lower(h1.Ticker) as Ticker, h1.SecurityReturn, 
                    LEFT(h1.Sector,2) As Sector, LEFT(h1.Sector,4) As SubSector, h1.Sector As Industry,
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
                    case AdventOutputType.Constituent:
                        SqlOrderBy = @"
                        ORDER BY Ticker
                        ";
                        break;
                    case AdventOutputType.Sector:
                        SqlOrderBy = @"
                        ORDER BY Industry
                        ";
                        break;
                    default:
                        break;
                }

                //LogHelper.WriteLine(SqlSelect);
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
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (ReturnsGenerated);
        }


        public bool GetNextConstituentReturn(out string sCusip, out string sTicker,
                                             out string sSector, out string sSubSector, out string sIndustry,
                                             out string sWeight, out string sSecurityReturn)
        {
            string sMsg = null;
            bool GetNext = false;
            sCusip = "";
            sTicker = "";
            sSector = "";
            sSubSector = "";
            sIndustry = "";
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
                        sOriginalTicker = sOriginalTicker.ToLower();
                        sWeight = mSqlDr["Weight"].ToString();
                        sSecurityReturn = mSqlDr["SecurityReturn"].ToString();
                        sSector = mSqlDr["Sector"].ToString();
                        sSubSector = mSqlDr["SubSector"].ToString();
                        sIndustry = mSqlDr["Industry"].ToString();
                        string sFileDate = mSqlDr["FileDate"].ToString(); ;

                        ConstituentCount += 1;
                        //string sCmpCusip = null;
                        //string sNewCusip = null;
                        //bool Done = false;
                        // JK: Not sure why this was written this way as it ends in an infinite loop
                        //for (sCmpCusip = sCusip, sNewCusip = ""; !Done;)
                        //{
                        //    sNewCusip = GetNewCUSIP(sCmpCusip);
                        //    if (sCmpCusip.Equals(sNewCusip))
                        //        Done = true;
                        //    else
                        //        sCmpCusip = sNewCusip;
                        //}
                        //string smCusip = null;
                        //if (sCusip.Equals(sNewCusip))
                        //    smCusip = sCusip;
                        //else
                        //    smCusip = sNewCusip;

                        string sNewCusip = GetNewCUSIP(sCusip);
                        string smCusip = null;
                        if (sCusip.Equals(sNewCusip))
                            smCusip = sCusip;
                        else
                            smCusip = sNewCusip;


                        string smTicker = GetSecurityMasterTicker(smCusip, sFileDate);
                        if (smTicker.Length > 0)
                            sTicker = smTicker;
                        else
                            sTicker = sOriginalTicker;

                        sMsg = sTicker + "," + sWeight + "," + sSecurityReturn + "," + sCusip;
                        //LogHelper.WriteLine(sMsg);


                        if ((mPrevId.Length > 0) && sCusip.Equals(mPrevId))
                        {
                            sMsg = "GetNextConstituentReturn: duplicate, " + sCusip;
                            LogHelper.WriteLine(sMsg);
                        }
                        mPrevId = sCusip;
                    }
                    else
                    {
                        //sMsg = "GetNextConstituentReturn:," + ConstituentCount.ToString();
                        //LogHelper.WriteLine(sMsg);
                        if (ConstituentCount > 0)
                        {
                            CloseGlobals();
                        }
                    }
                }
            }

            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
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
                    LogHelper.WriteLine(sMsg);
                }
            }


            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                //swLogFile.Flush();
                //LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (GetNext);
        }
        /*
        void dummy( string EndDate, string SelectedItem)
        {
            if (GenerateConstituentReturnsForDate(EndDate, SelectedItem))
                for (bool GotNext = true; GotNext;)
                {
                    string sCusip = null;
                    string sTicker = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if ((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker, out sWeight, out sSecurityReturn)) == true)
                    {
                    }
                }
            if (GenerateSectorReturnsForDate(EndDate, SelectedItem))
                for (bool GotNext = true; GotNext;)
                {
                    string sSector = null;
                    string sWeight = null;
                    string sReturn = null;

                    if ((GotNext =
                        GetNextSectorReturn(out sSector, out sWeight, out sReturn)) == true)
                    {
                    }
                }
            if (GenerateSubSectorReturnsForDate(EndDate, SelectedItem))
                for (bool GotNext = true; GotNext;)
                {
                    string sSubSector = null;
                    string sWeight = null;
                    string sReturn = null;

                    if ((GotNext =
                        GetNextSubSectorReturn(out sSubSector, out sWeight, out sReturn)) == true)
                    {
                    }
                }
            if (GenerateIndustryReturnsForDate(EndDate, SelectedItem))
                for (bool GotNext = true; GotNext;)
                {
                    string sIndustry = null;
                    string sWeight = null;
                    string sReturn = null;

                    if ((GotNext =
                        GetNextIndustryReturn(out sIndustry, out sWeight, out sReturn)) == true)
                    {
                    }
                }

        }
        */

        private double CalculateAdventTotalReturnForDate(string sDate, string sIndexName, bool bSaveReturnInDb)
        {
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;
            double dReturn = 0.0;
            // string sMsg = null;

            try
            {
                //sMsg = "CalculateAdventTotalReturnForDate: ";
                //LogHelper.WriteLine(sMsg + sDate );
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

                //LogHelper.WriteLine(SqlSelect);
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
                        
                        //LogHelper.WriteLine(sDate + " " + sReturn);
                        if (bSaveReturnInDb)
                        {
                            foreach (string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                            {
                                if (vendorFormat.Equals("SECTOR_LEVEL4") == false)  // Russell only has 3 sector levels (Snp has 4)
                                    sharedData.AddTotalReturn(oDate, sIndexName, Vendors.Russell.ToString(), vendorFormat, dReturn, "AdvReturnDb");
                            }
                        }
                    }
                }
            }

            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
                //swLogFile.Flush();
                //LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }

            return (dReturn);
        }



        public string GetNewCUSIP(string sOldCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;
            string sNewCUSIP = sOldCUSIP;

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT newSymbol FROM HistoricalSymbolChanges WHERE oldSymbol = @oldSymbol
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
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }

            return (sNewCUSIP);
        }

        public string GetSecurityMasterTicker(string sCUSIP, string sFileDate)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;
            string sTicker = "";

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT Ticker FROM HistoricalSecurityMasterFull WHERE Cusip = @Cusip
                    and (@FileDate >= BeginDate and @FileDate <= EndDate)
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar, 8);
                cmd.Parameters["@Cusip"].Value = sCUSIP;
                cmd.Parameters.Add("@FileDate", SqlDbType.Date);
                cmd.Parameters["@FileDate"].Value = sFileDate;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        sTicker = dr["Ticker"].ToString();
                        sTicker = sTicker.ToLower();
                    }
                }

            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                dr.Close();
                cnSql.Close();
            }

            return (sTicker);
        }

    public void DeleteRussellTotalReturnForIndex(DateTime FileDate, string sIndexName)
    {
       SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            try
            {
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;
            cnSql.Open();
            SqlDelete = "delete FROM RussellDailyIndexReturns ";
            SqlWhere = "where FileDate = @FileDate and IndexName = @IndexName";
            cmd = new SqlCommand();
            cmd.Connection = cnSql;
            cmd.CommandText = SqlDelete + SqlWhere;
            cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
            cmd.Parameters["@FileDate"].Value = FileDate;
            cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
            cmd.Parameters["@IndexName"].Value = sIndexName;
            cmd.ExecuteNonQuery();
            cnSql.Close();
        }
        catch (SqlException ex)
        {
            if (ex.Number == 2627)
            {
                LogHelper.WriteLine(ex.Message);
            }
        }
        finally
        {
            cnSql.Close();
        }
    }



        public double GetVendorTotalReturnForDate(string sDate, string sIndexName)
        {
            SqlDataReader dr = null;
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            double dReturn = 0.0;
            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    select VendorReturn from TotalReturns
                    where IndexName = @IndexName and ReturnDate = @ReturnDate 
                    and Vendor = 'Russell' and VendorFormat = 'CONSTITUENT'
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
                        string s = dr["VendorReturn"].ToString();
                        if (s.Length > 0 && double.TryParse(s, out double dNum))
                            dReturn = Convert.ToDouble(dr["VendorReturn"].ToString());
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                cnSql.Close();
            }
            return (dReturn);
        }




        public void CalculateVendorTotalReturnsForPeriod(string sStartDate, string sEndDate, string sIndexName)
        {
            DateTime startDate = DateTime.Parse(sStartDate);
            DateTime endDate = DateTime.Parse(sEndDate);
            DateTime prevDate = DateTime.MinValue;

            for( DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
            {
                prevDate = DateHelper.PrevBusinessDay(date);

                try
                {
                    if (mSqlConn == null)
                    {
                        mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                        mSqlConn.Open();
                    }
                    string sTotalReturn = String.Empty;
                    string sTotalReturnPrev = String.Empty;
                    string SqlSelect = @"
                    select count(*) from RussellDailyIndexReturns
                    where IndexName = @IndexName 
                    and FileDate = @FileDate 
                    ";
                    SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                    cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                    //cmd.Parameters.Add("@TotalReturn", SqlDbType.VarChar);
                    cmd.Parameters["@IndexName"].Value = sIndexName;
                    cmd.Parameters["@FileDate"].Value = prevDate;
                    //cmd.Parameters["@TotalReturn"].Value = sTotalReturn;

                    int iCountPrev = (int)cmd.ExecuteScalar();
                    if (iCountPrev == 1)
                    {
                        cmd.Parameters["@FileDate"].Value = date;
                        int iCount = (int)cmd.ExecuteScalar();
                        if (iCount == 1)
                        {
                            SqlSelect = @"
                            select TotalReturn from RussellDailyIndexReturns
                            where IndexName = @IndexName 
                            and FileDate = @FileDate 
                            ";
                            cmd.CommandText = SqlSelect;
                            SqlDataReader dr = cmd.ExecuteReader();
                            if (dr.HasRows)
                            {
                                if (dr.Read())
                                {
                                    sTotalReturn = dr["TotalReturn"].ToString();
                                    sTotalReturn = sTotalReturn.Trim();
                                }
                            }
                            dr.Close();
                            cmd.Parameters["@FileDate"].Value = prevDate;
                            dr = cmd.ExecuteReader();
                            if (dr.HasRows)
                            {
                                if (dr.Read())
                                {
                                    sTotalReturnPrev = dr["TotalReturn"].ToString();
                                    sTotalReturnPrev = sTotalReturnPrev.Trim();
                                }
                            }
                            dr.Close();

                            double TotalReturn = Convert.ToDouble(sTotalReturn);
                            double TotalReturnPrev = Convert.ToDouble(sTotalReturnPrev);
                            double CalculatedTotalReturn = 100 * (TotalReturn / TotalReturnPrev - 1);
                            int Precision = 9;
                            CalculatedTotalReturn = Math.Round(CalculatedTotalReturn, Precision, MidpointRounding.AwayFromZero);

                            foreach (string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                            {
                                if (vendorFormat.Equals("SECTOR_LEVEL4") == false)  // Russell only has 3 sector levels (Snp has 4)
                                    sharedData.AddTotalReturn(date, sIndexName, Vendors.Russell.ToString(), vendorFormat, CalculatedTotalReturn, "VendorReturn");
                            }
                        }
                    }
                }
                catch (SqlException ex)
                {
                    if (ex.Number == 2627)
                    {
                        LogHelper.WriteLine(ex.Message);
                    }
                }
                finally
                {
                }
            }
        }

        public void CalculateAdjustedTotalReturnsForPeriod(string sStartDate, string sEndDate, string sIndexName)
        {

            DateTime startDate = DateTime.Parse(sStartDate);
            DateTime endDate = DateTime.Parse(sEndDate);

            for (DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
            {
                try
                {
                    if (mSqlConn == null)
                    {
                        mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                        mSqlConn.Open();
                    }
                    SqlDataReader dr = null;
                    double dVendorReturn = double.MinValue;
                    double dAdvReturn = double.MinValue;
                    //double dAdvAdjFactor = double.MinValue;
                    //double dAdvReturnAdj = double.MinValue;
                    //double dAdvReturnDb = double.MinValue;
                    double dDiff = double.MinValue;
                    //double dDiffAdj = double.MinValue;
                    //double dDiffDb = double.MinValue;
                    //double dCumltDiff = double.MinValue;
                    int iTotalReturnPrecision = 9;


                    string SqlSelect = @"
                    select VendorReturn, AdvReturnDb from TotalReturns
                    where IndexName = @IndexName and ReturnDate = @ReturnDate 
                    and Vendor = 'Russell' and VendorFormat = 'CONSTITUENT'
                    ";

                    SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                    cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                    cmd.Parameters["@IndexName"].Value = sIndexName;
                    cmd.Parameters["@ReturnDate"].Value = date;

                    dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        if (dr.Read())
                        {
                            double dNum;
                            string s = "";
                            s = dr["AdvReturnDb"].ToString();
                            if(s.Length > 0 && double.TryParse(s, out dNum))
                                dAdvReturn = Convert.ToDouble(dr["AdvReturnDb"].ToString());
                            s = dr["VendorReturn"].ToString();
                            if (s.Length > 0 && double.TryParse(s, out dNum))
                                dVendorReturn = Convert.ToDouble(dr["VendorReturn"].ToString());
                            dr.Close();

                            if((dVendorReturn.Equals(double.MinValue) == false) && (dAdvReturn.Equals(double.MinValue) == false))
                            {
                                dDiff = Math.Round((dVendorReturn - dAdvReturn), iTotalReturnPrecision, MidpointRounding.AwayFromZero);

                                foreach (string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                                    if (vendorFormat.Equals("SECTOR_LEVEL4") == false)  // Russell only has 3 sector levels (Snp has 4)
                                        sharedData.AddTotalReturn(date, sIndexName, Vendors.Russell.ToString(), vendorFormat, dDiff, "DiffDb");

                                //double dTest = Math.Round((dAdvReturn + dDiff), iTotalReturnPrecision, MidpointRounding.AwayFromZero);
                            }
                        }
                    }
                }
                
                catch (SqlException ex)
                {
                    if (ex.Number == 2627)
                    {
                        LogHelper.WriteLine(ex.Message);
                    }
                }
                finally
                {
                }
            }
        }


        /***************************************************/
#endregion Testing Code used by Form2.Russell Tab
#if commented

            List<IndexRow> indexRows = new List<IndexRow>();

            indexRows = new List<IndexRow>();

            indexRowsAdded = indexRows.Count;

            fileData.IndexRowsStart();

            foreach (string line in fileData.InputFile.Lines)
            {
                //ConsoleWriter.cWriteInfo("Line: " + line );
                fileData.GetIndexRows(line, indexRows, cusipsProcessed, tickersProcessed);
                lineNumber++;

           fileData.IndexRowsFinish();

                indexRows.Sort(new IndexRowComparer());


        /// <summary>
        /// add/copy dummy indexRows with zero return
        /// if between last biz date and month end no biz days
        /// added by Sasha 05/28/2009 - new specs by Susan Koo
        /// </summary>
        /// <param name="indexRows">collection to modify</param>
        /// <param name="dtFrom">change start date to the last biz day</param>
        /// <param name="dtTo">change end date to the month end, which should be NON biz day</param>
        /// <param name="iCopyRows">number of rows to copy (copy from the end of collection)</param>
        private void insertDummyIndexRows(List<IndexRow> indexRows, DateTime dtFrom, DateTime dtTo, int iCopyRows)
        {
            if (dtFrom == dtTo)
                return;

            // duplicate last indexRowsAdded with the dummy irr/weights...
            int indexRowsStartFrom = indexRows.Count - iCopyRows;
            for (int i = 0; i < iCopyRows; i++)
            {
                IndexRow indexRowBase = indexRows[indexRowsStartFrom + i];
                IndexRow indexRow = new IndexRow(indexRowBase, dtFrom, dtTo, indexRowBase.Weight, 0);
                indexRows.Add(indexRow);
            }
        }


#region ValidateSectorReturns

        /// <summary>
        /// Validate the sector returns for 
        ///    S and P (all but StandardPoorsSecurity);
        ///    RussellSector
        /// </summary>
        /// <param name="indexRows"></param>
        /// <param name="outputPathNames"></param>
        private void ValidateSectorReturns(List<IndexRow> indexRows, List<string> outputPathNames)
        {
            /// 1. See if there is anything to do.
            ///     all StandardPoors... except StandardPoorsSecurity
            ///     RussellSector (added on 8/3/2009) 
            if ((inputFormat.vendor == Vendor.StandardAndPoors &&
                                 inputFormat.inputFormatId != InputFormatId.StandardPoorsSecurity) ||
                 inputFormat.inputFormatId == InputFormatId.RussellSector)
                ConsoleWriter.cWriteInfo("ValidateSectorReturns: " + inputFormat.inputFormatId);
            else
                return;

            // 2. Determine the needed Ids and dates.
            Dictionary<string, byte> neededDates = new Dictionary<string, byte>();
            Dictionary<string, byte> neededIds = new Dictionary<string, byte>(StringComparer.Ordinal);
            Dictionary<string, byte> neededItems = new Dictionary<string, byte>(StringComparer.Ordinal);

#region add IndexRow indexRow in indexRows

            foreach (IndexRow indexRow in indexRows)
            {
                // Use the GICS code for the neededIds.  Only validate the SP500.
                //if (!inputFormat.GetVendorIndexId(indexRow.IndexId).Equals("SP500", StringComparison.OrdinalIgnoreCase)) { continue; }
                if (!neededIds.ContainsKey(indexRow.IndexId))
                    neededIds.Add(indexRow.IndexId, 0);

                if (!neededItems.ContainsKey(indexRow.ItemId))
                    neededItems.Add(indexRow.ItemId, 0);

                if (!neededDates.ContainsKey(indexRow.BeginningDateYyyymmdd))
                    neededDates.Add(indexRow.BeginningDateYyyymmdd, 0);

                if (!neededDates.ContainsKey(indexRow.EndingDateYyyymmdd))
                    neededDates.Add(indexRow.EndingDateYyyymmdd, 0);
            }

#endregion

            // 3. Fetch the total return levels for the needed Ids and dates.
            Dictionary<string, double> totalReturnLevels = Utility.getNewDictStrDbl(0);
            Dictionary<string, string> itemDescriptions = Utility.getNewDictStrStr(0);

            // fill totalReturnLevels, itemDescriptions  [Key=id+'\a'+date]
            foreach (string neededDate in neededDates.Keys)
                inputFormat.FetchPublishedTotalReturnLevel(neededDate, neededIds, neededItems, totalReturnLevels, itemDescriptions);

            if (!SKIP_CLASSIFICATION_RETURNS || !SKIP_CLASSIFICATION_RETURNS_DB) // will be written in there is data
            {
#region 4. Write the report [CLASSIFICATION_RETURNS_TXT].

                string pathName = Path.Combine(_OutputFolder, CLASSIFICATION_RETURNS_TXT);

#region make xml string sClassReturns - loop thru indexRow in indexRows

                int nCR = 0;
                string sClassReturns = "<Document><!-- id[Item]; bd[From Date]; ed[To Date]; " +
                                       "p[Published Return]; c[Rolled-Up Return]; d[Difference] -->";

                foreach (IndexRow indexRow in indexRows)
                {
                    //if (!inputFormat.GetVendorIndexId(indexRow.IndexId).Equals("SP500", StringComparison.OrdinalIgnoreCase)) { continue; }
                    double totalReturnLevelBegin;
                    double totalReturnLevelEnd = Utility.NullDouble;
                    double publishedTotalReturn = Utility.NullDouble;
                    string beginningDateKey = indexRow.IndexId + Utility.InternalDelimiter + indexRow.ItemId + Utility.InternalDelimiter + indexRow.BeginningDateYyyymmdd;
                    string endingDateKey = indexRow.IndexId + Utility.InternalDelimiter + indexRow.ItemId + Utility.InternalDelimiter + indexRow.EndingDateYyyymmdd;
                    string endingDateKeyVendor = getVendorIndexName(indexRow.IndexId) + Utility.InternalDelimiter + indexRow.ItemId + Utility.InternalDelimiter + indexRow.EndingDateYyyymmdd;
                    string itemDescription;

                    // get itemDescription by client index id or vendor index id
                    itemDescriptions.TryGetValue(endingDateKey, out itemDescription);
                    if (string.IsNullOrEmpty(itemDescription))
                        itemDescriptions.TryGetValue(endingDateKeyVendor, out itemDescription);

                    // get totalReturnLevelBegin, totalReturnLevelEnd
                    if (totalReturnLevels.TryGetValue(beginningDateKey, out totalReturnLevelBegin) &&
                        totalReturnLevels.TryGetValue(endingDateKey, out totalReturnLevelEnd))
                        publishedTotalReturn = 100 * (totalReturnLevelEnd / totalReturnLevelBegin - 1);

                    // RussellSector is in another situation - returns should be taken as is fromDSS_RGS files...
                    if (inputFormat.vendorFormatId.EndsWith("RussellSector"))
                        publishedTotalReturn = totalReturnLevelEnd;

                    // when using bulk insert , dates should be provided in yyyy-MM-dd format
                    if (publishedTotalReturn != Utility.NullDouble)
                        sClassReturns += TABLE_TAG_OPEN + "<!-- " + (nCR++).ToString() + " -->" +
                           makeTagNode("Item", itemDescription) +
                           makeTagNode("FromDate", Utility.DateToYyyy_mm_dd(indexRow.BeginningDate)) +
                           makeTagNode("ToDate", Utility.DateToYyyy_mm_dd(indexRow.EndingDate)) +
                           makeTagNode("PublReturn", Math.Round((double)publishedTotalReturn, 4)) +
                           makeTagNode("RlUpReturn", Math.Round(indexRow.RateOfReturn, 4)) +
                           makeTagNode("Diff", Math.Round((double)publishedTotalReturn - indexRow.RateOfReturn, 4)) +
                           TABLE_TAG_CLOSE;
                    //(Math.Abs(showDifference) < 0.001 ? string.Empty : showDifference.ToString(CultureInfo.InvariantCulture)));
                }

                sClassReturns += "</Document>";

#endregion

                XmlDocument docClassReturns = Utility.getNewXmlDoc(sClassReturns);

                if (!SKIP_CLASSIFICATION_RETURNS_DB)
                {
                    SqlXml.BulkInsertXMLIntoTable(docClassReturns, SqlXml.SCHEMA_TABLE.ClassificationReturns);
                    LogHelper.Log("Classification Returns added into table ClassificationReturns", LogHelper.CATEGORY_SOURCES.Info);
                }

                if (!SKIP_CLASSIFICATION_RETURNS)
                {
                    if (nCR > 0)
                    {
                        // save as xml document
                        //docClassReturns.Save(pathName + ".xml");
                        //RewriteXMLtoAXML(docClassReturns, pathName + ".xml");

                        bool bClassReturnsExists = File.Exists(pathName);

                        // save text file
                        using (TextWriter tw = new StreamWriter(pathName, true))
                        {
                            if (!bClassReturnsExists)
                                tw.WriteLine("Item\tFrom Date\tTo Date\tPublished Return\tRolled-Up Return\tDifference");

                            foreach (XmlNode nd in docClassReturns.SelectNodes("//Table"))
                                tw.WriteLine(Utility.getNodeTextTabbed(nd, "id,bd,ed,p,c,d"));

                            tw.Close();
                        }
                        ConsoleWriter.cWriteInfo("CLASSIFICATION_RETURNS_TXT " + pathName + ", added pathName " + pathName);
                        AddOutputPathName(outputPathNames, pathName);
                    }
                    else
                        ConsoleWriter.cWriteWarning("CLASSIFICATION_RETURNS_TXT not written, no data: " + pathName);
                }

#endregion
            }
        }

#endregion


        /// <summary>
        /// Write the report. (writes TotalReturns.txt)
        /// </summary>
        /// <param name="outputPathNames"></param>
        /// <param name="totalReturns"></param>
        /// <param name="totalReturnLevels"></param>
        private void writeTotalReturns(
            List<string> outputPathNames,
            Dictionary<string, double> totalReturnsAdvent,
            Dictionary<string, double> totalReturnsVendor,
            string ReturnDbCol,
            string VendorFormat,
            double dAdvAdjFactor,
            out double AdventVsVendorDiff)
        {
            //if (totalReturnsAdvent.Count == 0)
            //{
            //    LogHelper.Log("Total Returns - no results", LogHelper.CATEGORY_SOURCES.General);
            //    return;
            //}
            AdventVsVendorDiff = Utility.NullDouble;
            string cumulativeIndexId = null;
            double cumulativeDifference = 0;
            List<string> totalReturnLines = new List<string>(totalReturnsAdvent.Count);
            string indexId = null;


            int totalReturnPrecision = 9;

            foreach (KeyValuePair<string, double> calculatedTotalReturn in totalReturnsAdvent)
            {
                // 1 = indexId, 3 = beginningDateString, 4 = e();ndingDateString
#region validate cumulativeIndexId

                string[] keyFields = calculatedTotalReturn.Key.Split(Utility.InternalDelimiter);
                if (cumulativeIndexId != keyFields[1])
                {
                    if (cumulativeIndexId != null)
                    {
                        totalReturnLines.Add(string.Empty);
                        cumulativeDifference = 0;
                    }
                    cumulativeIndexId = keyFields[1];
                }

#endregion

#region initialize misc values: publishedTotalReturnm, keyInit, ...

                /// to store value from All files (ALL20050308.txt, ALL20050309.txt):
                /// 
                ///     open excel files ALL20050308.txt (opening day) and ALL20050309.txt (closing day):
                ///     for given IndexId (RU1000, for example) and currency usd 
                /// 
                ///     publishedTotalReturn =  100*(opening total/closing total -1)
                ///     
                double publishedTotalReturn = Utility.NullDouble;

                string keyInit = keyFields[1] + Utility.InternalDelimiter;

                double totalReturnLevelBegin;
                double totalReturnLevelEnd = Utility.NullDouble;

                bool bBegin = totalReturnsVendor.TryGetValue(keyInit + keyFields[3], out totalReturnLevelBegin);
                bool bEnd = totalReturnsVendor.TryGetValue(keyInit + keyFields[4], out totalReturnLevelEnd);

                if (bBegin && bEnd)
                    publishedTotalReturn = 100 * (totalReturnLevelEnd / totalReturnLevelBegin - 1);


#region writing calc logic [if needed]

                string vIndName = Utility.getVendorIndexName(keyFields[1]);
                string totRetLBegin = "\n\t " + totalReturnLevelBegin.ToString().PadRight(14) +
                    " = totalReturnLevelBegin value is in " + vIndName;
                string totRetLEnd = "\n\t " + totalReturnLevelEnd.ToString().PadRight(14) +
                    " = totalReturnLevelEnd   value is in " + vIndName;
                string sPubRet = "\n\t publishedTotalReturn = 100 * (totalReturnLevelEnd / totalReturnLevelBegin - 1)  =  " +
                    publishedTotalReturn.ToString() + " = 100 * (" + totalReturnLevelEnd.ToString() + " / " +
                    totalReturnLevelBegin.ToString() + " - 1)\n\n================ End of Total Return Logic Processing";

                if (inputFormat.vendor == Vendor.StandardAndPoors)
                    Processor.WriteCalcLog("Processor", "WriteTotalReturns (compare TotalReturn to publishedTotalReturn) " +
                        totRetLBegin + "(TR) row, 'Index Value' column, opening file " + keyFields[3] + "_" + vIndName + FileData._ICL +
                        totRetLEnd + "(TR) row, 'Index Value' column, closing file " + keyFields[4] + "_" + vIndName + FileData._ICL +
                        sPubRet + " StandardAndPoors ================ \n");
                else if (inputFormat.vendor == Vendor.Russell)
                    Processor.WriteCalcLog("Processor", "WriteTotalReturns (compare TotalReturn to publishedTotalReturn) " +
                        totRetLBegin + " USD row, 'TOTAL' column, opening file ALL" + keyFields[3] + ".TXT" +
                        totRetLEnd + " USD row, 'TOTAL' column, closing file ALL" + keyFields[4] + ".TXT" +
                        sPubRet + " Russell ================ \n");

#endregion

                /// totalReturnLevelEnd    2797.25271;  totalReturnLevelBegin  2826.02617
                /// publishedTotalReturn -1.0181597150602406  match to John's
                /// ... calculatedTotalReturn.Value DOES NOT MATCH...
                /// calculatedTotalReturn.Value = -1.0169890276602902, s/b publishedTotalReturn  

#endregion

#region make xml <Table> node for table totalReturnsAdvent

                if (publishedTotalReturn != Utility.NullDouble)
                {
                    double difference = publishedTotalReturn - calculatedTotalReturn.Value;
                    AdventVsVendorDiff = difference;
                    cumulativeDifference += difference;

                    if (keyFields[1] != indexId)
                        indexId = keyFields[1];

                    string publTotReturn = Math.Round(publishedTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero).ToString();
                    string calcTotReturn = Math.Round(calculatedTotalReturn.Value, totalReturnPrecision, MidpointRounding.AwayFromZero).ToString();
                    string cumlDiff = Math.Round(cumulativeDifference, totalReturnPrecision, MidpointRounding.AwayFromZero).ToString();
                    string diff = Math.Round(difference, totalReturnPrecision, MidpointRounding.AwayFromZero).ToString();

                    if (!SKIP_TOTAL_RETURNS_DB)
                    {
                        string sIndexName = keyFields[1];
                        string sDate = keyFields[4];
                        sDate = sDate.Substring(4, 2) + "/" + sDate.Substring(6, 2) + "/" + sDate.Substring(0, 4);

                        //RussellData oRussell = new RussellData();
                        double dRolledUpReturn =
                            Math.Round(calculatedTotalReturn.Value, totalReturnPrecision, MidpointRounding.AwayFromZero);
                        double dPublishedReturn =
                            Math.Round(publishedTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);
                        double dDiff =
                            Math.Round(difference, totalReturnPrecision, MidpointRounding.AwayFromZero);
                        double dCumltDiff =
                            Math.Round(cumulativeDifference, totalReturnPrecision, MidpointRounding.AwayFromZero);
                        if (ReturnDbCol.Equals("AdvReturn"))
                        {
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dPublishedReturn, "VendorReturn");
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dRolledUpReturn, "AdvReturn");
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dDiff, "Diff");
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dCumltDiff, "CumltDiff");
                        }
                        else if (ReturnDbCol.Equals("AdvReturnAdj"))
                        {
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dRolledUpReturn, "AdvReturnAdj");
                            dAdvAdjFactor = Math.Round(dAdvAdjFactor, totalReturnPrecision, MidpointRounding.AwayFromZero);
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dAdvAdjFactor, "AdvAdjFactor");
                            mRussell.AddTotalReturn(sDate, sIndexName, VendorFormat, dDiff, "DiffAdj");
                        }
                    }
                }
#endregion
            }
        }

        /// <summary>
        /// Write all reports and data files.[called from Process2() method]
        /// writes total returns.
        /// </summary>
        /// <param name="indexRows"></param>
        /// <param name="outputPathNames"></param>
        private void WriteOutput(List<IndexRow> indexRows, List<string> outputPathNames )
        {
            WriteCalcLog("Processor", "WriteOutput  Change the weights to sum to 100 and calculate the totalReturnsAdvent." +
                "\n\t for (int j = dayI; j < i; j++) indexRows[j].Weight /= dayWeight;");

            /// indexRows[0].ItemId should be non empty!  
            /// A first chance exception of type 'System.Collections.Generic.KeyNotFoundException' occurred in mscorlib.dll
            /// 
            Dictionary<string, DateTime> finalEndingDatesKeyedByIndexId = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            Dictionary<string, double> totalReturnsAdvent = Utility.getNewDictStrDbl(0, false);
            Dictionary<string, double> totalReturnsVendor = Utility.getNewDictStrDbl(0);
            double AdventVsVendorDiff = Utility.NullDouble;

            IndexRow indexRow = null;
            string indexRowDayKey = null;
            string dayKey = null;
            int dayI = int.MinValue;
            int i = 0;
#region 1. Change the weights to sum to 100 and calculate the totalReturnsAdvent.

            //debugger.DebugConsole("2. ------ indexRowsConsolidated input file " + Path.GetFileName(mFileDatas[0].pathName));
            //FileData.DebugIndexRows(indexRowsConsolidated, 1, 4, true);

            if (mRussell == null)
            {
                mRussell = new RussellData();
            }


            for ( i = 0; i <= indexRows.Count; i++)
            {
                if (i != indexRows.Count)
                {
                    indexRow = indexRows[i];
                    indexRowDayKey = indexRow.GetDatePeriodKey();
                }
                //if ((indexRows[i].Security.Ticker.ToString() == "IMB") || (indexRows[i].Security.Cusip.ToString() == "45660710"))
                //{
                //    Debugger.Break();
                //}


                if (indexRowDayKey != dayKey || i == indexRows.Count)
                {
                    // You are on a new day, so process the previous day.
                    if (i != 0)
                    {
                        bool weightIsMktValue = indexRows[dayI].WeightIsMktValue;
                        //for (int j = dayI; j < i; j++)
                        //{
                        //    WriteCalcLog("", indexRows[j].Security.Cusip.ToString() + "|" + indexRows[j].Security.Ticker.ToString() + "|" + j.ToString().PadRight(5) + "|" + indexRows[j].Weight.ToString());
                        //}
                        uint ConstituentCount = 0;
                        if (weightIsMktValue)
                        {
                            double totalMktValue = 0;
                            for (int j = dayI; j < i; j++)
                            {
                                totalMktValue += indexRows[j].Weight;
                                ConstituentCount += 1;
                            }
                            for (int j = dayI; j < i; j++)
                            {
                                indexRows[j].Weight /= totalMktValue;
                                indexRows[j].Weight *= 100;
                                indexRows[j].ConstituentCount = ConstituentCount;
                            }
                        }
                        if (!SKIP_TOTAL_RETURNS || !SKIP_TOTAL_RETURNS_DB)
                        {
                            TotalReturnAdd(indexRows, dayI, i, totalReturnsAdvent);
                            ValidateTotalReturns(totalReturnsAdvent, totalReturnsVendor, outputPathNames, 
                                                 "AdvReturn", inputFormat.inputFormatId.ToString(),
                                                 (double) 0.0, out AdventVsVendorDiff);
//                            bool Correct = false;
//                            if (Correct)
                            if (inputFormat.vendor == Vendor.Russell)
                            {
                                if (AdventVsVendorDiff != Utility.NullDouble)
                                {
                                    double AddlContribution = AdventVsVendorDiff / ConstituentCount;
                                    for (int j = dayI; j < i; j++)
                                    {
                                        indexRows[j].RateOfReturnAdjustment = 100 * (AddlContribution / indexRows[j].Weight);
                                        indexRows[j].RateOfReturn += indexRows[j].RateOfReturnAdjustment;
                                    }
                                    totalReturnsAdvent.Clear();
                                    totalReturnsVendor.Clear();
                                    TotalReturnAdd(indexRows, dayI, i, totalReturnsAdvent);
                                    ValidateTotalReturns(totalReturnsAdvent, totalReturnsVendor, outputPathNames,
                                                         "AdvReturnAdj", inputFormat.inputFormatId.ToString(),
                                                         AddlContribution, out AdventVsVendorDiff);
                                }
                            }
                            ValidateSectorReturns(indexRows, outputPathNames);
                            totalReturnsAdvent.Clear();
                            totalReturnsVendor.Clear();
                            AdventVsVendorDiff = Utility.NullDouble;
                        }
                    }

#region Set the finalEndingDate.

                    DateTime finalEndingDate = Utility.NullDateTime;
                    try
                    {
                        if (!finalEndingDatesKeyedByIndexId.TryGetValue(indexRow.IndexId, out finalEndingDate))
                            finalEndingDatesKeyedByIndexId.Add(indexRow.IndexId, indexRow.EndingDate);
                        else if (finalEndingDate < indexRow.EndingDate)
                            finalEndingDatesKeyedByIndexId[indexRow.IndexId] = indexRow.EndingDate;
                    }
                    catch (Exception ex) { LogHelper.Log(new StackTrace(true), ex); /* throw ex; */ }

                    // Initialize the fields for a new day.
                    dayKey = indexRowDayKey;
                    dayI = i;

#endregion
                }
            }

            if (mRussell != null)
            {
                mRussell.RussellData_Finish();
                mRussell = null;
            }


#endregion

            /// 3. Possibly convert daily to monthly.
            /// indexRows[0].ItemId should be non empty!  - also should be converted with symbol changes shains
            indexRows = Period.ConsolidatePeriods(indexRows, inputFormat);

#region 4. Write the output. (WriteFilesAXML or WriteFilesAPXIX if _bWriteApxixFiles=true)

            if (_bWriteApxixFiles)
            {
                WriteFilesAPXIX(indexRows, outputPathNames);
                ConsoleWriter.cWriteWarningM("sOutFiles(APXIX) " + listFileTimePath(outputPathNames));
            }
            else
            {
                WriteFilesAXML(indexRows, finalEndingDatesKeyedByIndexId, outputPathNames);
                ConsoleWriter.cWriteWarning("sOutFiles(AXML) " + listFileTimePath(outputPathNames));
            }

            bool bAddFileOneByOne = false;

            if (bAddFileOneByOne)
            {   ///add file to database log using dal layer - one file at a time
                ///
#region call stored procedure sp_addFileHistory for each file (clear and init - above)

                sqlHelper.CleanRequests();
                sqlHelper.MakeRequestWParamDoc("sp_addFileHistory");
                foreach (string sfile in outputPathNames)
                {
                    // get the file size in KB: {0:##,###.00}
                    string comment = "WriteFilesAXML(); file size " + String.Format("{0:#,0}KB",
                        ((new FileInfo(sfile)).Length / 1024)).PadLeft(9);

                    if (_bWriteApxixFiles)
                        comment = comment.Replace("AXML", "APXIX");

                    sqlHelper.SetPrmText("@p_filePath", sfile);
                    sqlHelper.SetPrmText("@p_importType", "outputPathNames");
                    sqlHelper.SetPrmText("@p_comment", comment);
                    sqlHelper.SetPrmText("@p_batchID", inputFormat.batchID);

                    sqlHelper.Execute();
                    if (sqlHelper.HasErrors())
                        ConsoleWriter.cWriteWarningM(new StackTrace(true), sqlHelper.GetError());
                    // LogHelper.cWriteError(sqlHelper.GetError());
                }

#endregion
            }
            else
            {
                string sXML = listFileTimePath(outputPathNames, true, _bWriteApxixFiles);
                // debugger.DebugXML(sXML);
            }
#endregion
        }

        // AXML:  Replace by proper xml handling (for AXML) and outline using xsl...
        /// <summary>
        /// Main method to save indexRows collection as axml file.
        /// Write one AXML file for each index, and add the pathnames of the AXML files to outputPathNames.
        /// Calls RewriteXMLtoAXML to reformat xml to axml in the end.
        /// </summary>
        /// <param name="indexRows"></param>
        /// <param name="finalEndingDatesKeyedByIndexId"></param>
        /// <param name="outputPathNames"></param>
        private void WriteFilesAXML(List<IndexRow> indexRows,
                                    Dictionary<string, DateTime> finalEndingDatesKeyedByIndexId,
                                    List<string> outputPathNames)
        {
#region COMMENT: AXML sample
            /*
                <?xml version='1.0'?>
                <AdventXML version='3.0'>
                <AccountProvider name='Russell' code='RU'>
                <Imports/>
                <InterpretFXRates/>
                <XSXList index='RU3000V' date='20080108' batch='3'>
                <XSX from='20071231' through='20080102' indexperfiso='usd' type='cs' iso='usd' 
                             symbol='aa' weight='0.41768709' irr='0'/>
                            ...
                </XSXList>
                </AccountProvider>
             */
#endregion

            // DebugIndexRows (indexRows,10);
            // DebugIndexRows(indexRows);

            if (indexRows.Count == 0)
                return;

            XmlDocument doc = null;
            XmlElement elAccountProvider = null;
            XmlElement elSuffixList = null;     // XNXList node or XSXList node 
            XmlElement elSuffixPeriod = null;   // XNXPeriod or XSXPeriod node           
            XmlElement elSuffixDetail = null;   // XNXDetail node or XSXDetail node 

            // XNXList or XSXList
            string suffixListName = inputFormat.AcdFileSuffix + "List"; //...AcdFileSuffix = "XNX" or "XSX"

            // Notes:  This method could use XmlTextWriter, but XmlTextWriter is VERY slow.
            // 1. Declare working variables.
            ushort _indexIdOrdinal = Utility.NullUShort;
            string _indexPathName = "";

            /// key to keep previous date period
            //string _datePeriodKey = null;
            string _sFromTo = null;

            // list should be sorted (by dates) via indexRows.Sort(new IndexRowComparer());

            lstDATEDAYS = new Dictionary<string, DATEDAYS>();

            // indexRows[0].ItemId should be non empty! 

#region Loop through each indexRow


            if (mRussell == null)
            {
                mRussell = new RussellData();
            }
            
            string sClasses = "";
            int nClasses = 0;
            int indexRowCounter = 0;

            // debug index rows
            // FileData.DebugIndexRows (indexRows,10);

            // indexRow must have indexRow.ItemId non empty!
            foreach (IndexRow indexRow in indexRows)
            {
                indexRowCounter++;

                string sFromTo = indexRow.BeginningDateYyyymmdd + "|" + indexRow.EndingDateYyyymmdd;

#region fill lstDATEDAYS by different dates and biz dates counter

                if (!lstDATEDAYS.ContainsKey(sFromTo))
                {
                    string sDateBegin = indexRow.BeginningDateYyyymmdd;
                    string sDateEnd = indexRow.EndingDateYyyymmdd;

                    int days = inputFormat.getWorkingDays(inputFormat.vendor, sDateBegin, sDateEnd);

                    bool bHoliday = !Utility.IsBusinessDay(sDateBegin, inputFormat.vendor);
                    bool bEndOfMonth = Utility.IsEndOfMonth(sDateBegin);

                    /// if sDateBegin is end of month and not biz day , but exists as an end date for the
                    /// previous period, it means the it was artificially shifted and originally was biz day
                    ///  - that is , number of working days should be increased by 1
                    foreach (string sKeyDate in lstDATEDAYS.Keys)
                        if (sKeyDate.EndsWith(sDateBegin) && bEndOfMonth && bHoliday)
                        {
                            days++;
                            break;
                        }

                    string sDateFrom = getWorkingDayMonthEndBegin(MONTH_END_BEGIN.month_end, sDateBegin);
                    string sDateThrough = getWorkingDayMonthEndBegin(MONTH_END_BEGIN.month_end, sDateEnd);

                    lstDATEDAYS.Add(sFromTo, new DATEDAYS(sDateFrom, sDateThrough, days));

                    //ConsoleWriter.cWriteInfo("lstDATEDAYS.Add(sFromTo): " + sFromTo + "; days " + days.ToString());
                }

#endregion

                //ConsoleWriter.cWriteInfo("lstDATEDAYS.Add(sFromTo): " + sFromTo + "; days " + days.ToString());
                //if (lstDATEDAYS[sFromTo].bizDays < 2)
                //    continue;

                try
                {
#region use, or create doc with <AccountProvider name=... code=...><XNXList index=... date=... batch=...>

                    if (indexRow.IndexIdOrdinal != _indexIdOrdinal)
                    {
                        _indexIdOrdinal = indexRow.IndexIdOrdinal;

                        if (doc != null)
                        {
#region save axml result and start new document if there is a new version (in file name)

                            // we are here when saving new version 002, 003
                            _indexPathName = Utility.getNodeText(doc, "//AdventXML/@file");

                            /// optional - remove file location from the root node
                            doc.SelectSingleNode("//AdventXML").Attributes.RemoveNamedItem("file");

                            // save axml result and start new document
                            RewriteXMLtoAXML(doc, _indexPathName);

                            if (!inputFormat.Combined)
                                AddOutputPathName(outputPathNames, _indexPathName);

#endregion
                        }
                        else
                            doc = new XmlDocument();

                        //Construct the pathName of the AXML file and create new doc
                        DateTime dtFinal = finalEndingDatesKeyedByIndexId[indexRow.IndexId];
                        dtFinal = getWorkingDayMonthEndExtended(dtFinal);

                        _indexPathName = getIndexPathName(indexRow, dtFinal);

                        // no biz days => _indexPathName is empty => nothing to write, exit
                        if (string.IsNullOrEmpty(_indexPathName))
                            return;

                        /// this will create new xmldocument, we want to reuse existng                      
                        /// doc = Utility.getNewXmlDoc(blankAdventXML(_indexPathName));

                        /// we want to reuse existng:     
                        /// debugger.DebugXML(doc)
                        doc.LoadXml(blankAdventXML(_indexPathName));

#region create/fill doc.elAccountProvider node by attributes: <AccountProvider name=... code=...

                        elAccountProvider = doc.DocumentElement.SelectSingleNode("AccountProvider") as XmlElement;
                        elAccountProvider.SetAttribute("name", inputFormat.vendor.ToString());
                        elAccountProvider.SetAttribute("code", inputFormat.AcdVendorPrefix);

#endregion

#region create/fill doc.elSuffixList: <XNXList index=... date=... batch=...>

                        /// <?xml..<AdventXML..<AccountProvider..<suffixListName index=... date=... batch=...
                        /// build XNXList node with attributes (suffixListName = "XNXList")
                        elSuffixList = doc.CreateElement(suffixListName);

                        // set XNXList attributes: index, index, batch
                        /// indexRow.IndexId    index="RU1000"; convert using maping to the client name ru1000
                        // moved here due new format (Sasha, 12/2008)
                        //elSuffixList.SetAttribute("index", getClientIndexName(indexRow.IndexId));
                        elSuffixList.SetAttribute("index", indexRow.IndexId); // already client name

                        elSuffixList.SetAttribute("date", Utility.DateToYyyymmdd(dtFinal));
                        elSuffixList.SetAttribute("batch", inputFormat.GetVendorIndexBatchId(indexRow.IndexId).ToString());

                        elAccountProvider.AppendChild(elSuffixList);

#endregion

                        //FileData.DebugIndexRows (indexRows)
                        //// XNXPeriod node (originaly XNX)
                        elSuffixPeriod = doc.CreateElement(inputFormat.AcdFileSuffix + "Period");
                        elSuffixPeriod.SetAttribute("from", lstDATEDAYS[sFromTo].fromDate);
                        elSuffixPeriod.SetAttribute("through", lstDATEDAYS[sFromTo].throughDate);
                        elSuffixPeriod.SetAttribute("indexperfiso", inputFormat.IsoCurrencyCode);
                    }

#endregion

                    /// put proper handling for last biz day or month end!!!
                    /// do calc only for different days.

                    /// 
                    /// put <XSXPeriod from="01312004" through="20040202" indexperfiso="usd">
                    /// inside <XSXList index="r1000" date="20040202" batch="4">
#region if (indexRow.GetDatePeriodKey()!=_datePeriodKey) create elSuffixPeriod

                    //if (indexRow.GetDatePeriodKey() != _datePeriodKey)// && lstDATEDAYS[sFromTo].bizDays >0)
                    if (sFromTo != _sFromTo)// && lstDATEDAYS[sFromTo].bizDays >0)
                    {
                        /// 6. You are on a new date period, so construct the date PeriodString which will be 
                        /// repeated on every line.
                        /// indexRow.GetDatePeriodKey() = "1\a1\a0\a20040130\a20040131"
                        //_datePeriodKey = indexRow.GetDatePeriodKey();
                        _sFromTo = sFromTo;

                        // XNXPeriod node (originaly XNX)
                        elSuffixPeriod = doc.CreateElement(inputFormat.AcdFileSuffix + "Period");
                        elSuffixPeriod.SetAttribute("from", lstDATEDAYS[sFromTo].fromDate);
                        elSuffixPeriod.SetAttribute("through", lstDATEDAYS[sFromTo].throughDate);
                        elSuffixPeriod.SetAttribute("indexperfiso", inputFormat.IsoCurrencyCode);

                        string sClassNameXmlValid = Utility.XmlValidString(indexRow.ClassificationName);

                        if (!string.IsNullOrEmpty(sClassNameXmlValid) && !sClasses.Contains("|" + sClassNameXmlValid + "|"))
                        {
                            sClasses += "|" + sClassNameXmlValid + "|";
                            nClasses++;
                        }
                        //else
                        //    ConsoleWriter.cWriteWarningM(new StackTrace(true), "sClasses missing indexRow.ClassificationName");

                        //elSuffixDatePeriodString = elSuffix.Clone() as XmlElement ;
                    }
                    //else
                    //    elSuffixPeriod = elSuffixPeriod.Clone() as XmlElement;

#endregion

                    ///indexRow.ClassificationName is null for security
                    string className = indexRow.ClassificationName;
                    if (!inputFormat.ClassificationMustBeSecurity && !string.IsNullOrEmpty(className) &&
                                             !string.IsNullOrEmpty(Utility.XmlValidString(className)))
                        elSuffixPeriod.SetAttribute("class", Utility.XmlValidString(className));

                    // this line is for checking
                    if (nClasses > 1)
                        ConsoleWriter.cWriteWarningM("Classification contains more than one class!" + nClasses.ToString());

                    // add XNXPeriod node to XNXList node
                    if (lstDATEDAYS[sFromTo].bizDays > 0 || Utility.IsEndOfMonth(indexRow.EndingDateYyyymmdd))
                    {
                        //elSuffixList.InnerXml += elSuffixPeriod.OuterXml;
                        elSuffixList.AppendChild(elSuffixPeriod);
                    }
                    //else
                    //    continue;

#region Write the detail XNXDetail node if (indexRow.Weight != 0)

                    if (indexRow.Weight != 0)
                    {
                        elSuffixDetail = doc.CreateElement(inputFormat.AcdFileSuffix + "Detail");

                        //Utility.XmlValidString(indexRow.ItemId);
                        string sItemId = Utility.htmlEncode(indexRow.ItemId);
                        //JK  
                        //indexRow.Security.Cusip
                        // This where we should insert the Vendor, Indexname, Date, CUSIP, Ticker, Weight and Return

                        string sItemId1 = inputFormat.securityMaster.Securities[indexRow.Security.Cusip].Ticker;

                        //bool found = false;
                        //if (inputFormat.securityMaster.Securities[indexRow.Security.Cusip].Ticker == "ACE" ||
                        //    inputFormat.securityMaster.Securities[indexRow.Security.Cusip].Ticker == "MMM" ||
                        //    inputFormat.securityMaster.Securities[indexRow.Security.Cusip].Ticker == "HAL")
                        //    found = true;

                        if (inputFormat.ClassificationMustBeSecurity)
                        {
                            if (indexRow.SecurityType != null)
                            {
                                elSuffixDetail.SetAttribute("type", indexRow.SecurityType.Substring(0, 2));
                                elSuffixDetail.SetAttribute("iso", inputFormat.IsoCurrencyCode);
                                elSuffixDetail.SetAttribute("symbol", sItemId);
                                AddSecurityMasterFull(sItemId, indexRow.Security.Cusip, indexRow.Security.Description, indexRow.Security.IndustryGroup, 
                                                 inputFormat.vendor.ToString(), indexRow.IndexId, lstDATEDAYS[sFromTo].throughDate);
                                


                            }
                            //else
                            //    ConsoleWriter.cWriteError("indexRow.SecurityType == null");
                        }
                        else
                        {
                            elSuffixDetail.SetAttribute("code", sItemId);

                            // debug RussellIndustry
                            //elSuffixDetail.SetAttribute("code3", sItemId.Substring(4));
                        }

                        elSuffixDetail.SetAttribute("weight", indexRow.Weight.ToString(NumberFormat, inputFormat.OutputCultureInfo));
                        elSuffixDetail.SetAttribute("irr", indexRow.RateOfReturn.ToString(NumberFormat, inputFormat.OutputCultureInfo));
                      


                        // append XNXDetail node to XNXPeriod node
                        //elSuffixPeriod.InnerXml += elSuffixDetail.OuterXml;
                        elSuffixPeriod.AppendChild(elSuffixDetail);
                    }

#endregion
                }
                catch (Exception ex)
                {
                    LogHelper.Log(new StackTrace(true), ex);
                    break;
                }

            } // next indexRow

            if (mRussell != null)
            {
                mRussell.RussellData_Finish();
                mRussell = null;
            }


#endregion

            try
            {
                if (doc != null)
                {
                    _indexPathName = Utility.getNodeText(doc, "//AdventXML/@file");

                    /// optional - remove file location from the root node
                    doc.SelectSingleNode("//AdventXML").Attributes.RemoveNamedItem("file");

                    //doc.Save(_indexPathName);
                    RewriteXMLtoAXML(doc, _indexPathName);

                    // make ref report - missing/updated/added securities (Russell/SnP)
#region if ref report has insturctions to create report - do it!

                    // if node is missing, treat it as "no ref report"
                    if (string.IsNullOrEmpty(referenceDataReport))
                        referenceDataReport = "<ReferenceDataReport txt='false' html='false' ftp='false' />";

                    XmlDocument docRefRep = Utility.getNewXmlDoc(referenceDataReport);

                    bool txtRefReport = Utility.getNodeBool(docRefRep, "//@txt");
                    bool htmlRefReport = Utility.getNodeBool(docRefRep, "//@html");

                    if (txtRefReport || htmlRefReport)
                    {
#region if (vendor == Russell, StandardAndPoors)

                        if (inputFormat.vendor == Vendor.Russell || inputFormat.vendor == Vendor.StandardAndPoors)
                        {
                            string refReportFile = Utility.writeVendorRefReport(inputFormat.vendor,
                                inputFormat.FilterFromDate, inputFormat.FilterToDate,
                                inputFormat.InputPathName, OutputFolder, false, txtRefReport, htmlRefReport);

                            ConsoleWriter.cWriteInfo("Writing Reference report for " +
                                inputFormat.vendor.ToString() + refReportFile);

                            ftpsite.Client = clientID;

                            // send file to ftp destination, if needed
                            foreach (string sRpt in Utility.SplitNL(refReportFile))
                                if (Utility.getNodeBool(docRefRep, "//@ftp"))
                                    Utility.FtpUpload(ftpsite, Utility.DateToYyyymmdd(inputFormat.FilterToDate), sRpt, jobName);
                        }
                        else
                        {
                            ConsoleWriter.cWriteColoredLine(
                                "Reference report implemented for Russell and StandardAndPoors only! ", ConsoleColor.Red,
                                "[tried: " + inputFormat.vendor.ToString() + "]", ConsoleColor.Green);
                        }

#endregion
                    }

#endregion

                    AddOutputPathName(outputPathNames, _indexPathName);
                }
                else
                    ConsoleWriter.cWriteWarningM(" doc == null, nothing to save: " + _indexPathName);
            }
            catch (Exception ex) { LogHelper.Log(new StackTrace(true), ex); }
        }


        /// <summary>
        /// Consolidate security rows into classification rows.
        /// </summary>
        /// <param name="unconsolidatedIndexRows"></param>
        /// <returns></returns>
        private List<IndexRow> ConsolidateIndexRows(List<IndexRow> unconsolidatedIndexRows)
        {
#region 1. If inputFormat.ClassificationMustBeSecurity, then there is nothing to consolidate.

            if (inputFormat.ClassificationMustBeSecurity)
            {
#if false
                // We can safely assume that there is only one security for each date period, because
                // they have been added to unique dictionaries along the way.
                // If we don't make this assumption, then this would be VERY slow.
                Dictionary<string, byte> itemKeys = newDictStrByte();
                foreach (IndexRow indexRow in unconsolidatedIndexRows)
                {
                    string itemKey = indexRow.GetPerformanceKey();
                    if (itemKeys.ContainsKey(itemKey))
                    {
                        throw new IndexDataException(ExceptionNumber.DuplicateSymbols, indexRow.ItemId, indexRow.BeginningDateYyyymmdd);
                    }
                    itemKeys.Add(itemKey, 0);
                }
#endif
                return unconsolidatedIndexRows;
            }

#endregion

#region 2. Consolidate the securities up to the classifications.

            /// Each record with the same key into one dictionary entry.
            /// For instance, you may have multiple industry records for Financials that you want to  
            /// consolidate into one dictionary entry.
            Dictionary<string, List<IndexRow>> consolidatedIndexRows =
                new Dictionary<string, List<IndexRow>>(StringComparer.Ordinal);

            ////debug
            //Dictionary<string, int> debugDistinctRows = new Dictionary<string, int>();

            foreach (IndexRow indexRow in unconsolidatedIndexRows)
            {
                string itemKey = indexRow.GetPerformanceKey(); // "1\a1\a1\a20090521\apius\aA"
                List<IndexRow> consolidatedIndexRow;

                if (!consolidatedIndexRows.TryGetValue(itemKey, out consolidatedIndexRow))
                {
                    consolidatedIndexRow = new List<IndexRow>();
                    consolidatedIndexRows.Add(itemKey, consolidatedIndexRow);
                    //debugDistinctRows.Add(itemKey, 1);
                }
                //else
                //    debugDistinctRows[itemKey]++;

                consolidatedIndexRow.Add(indexRow);

#region COMMENTED short code - not complete! initialization! "consolidatedIndexRow.Add(indexRow);"
                //string itemKey = indexRow.GetPerformanceKey();
                //if (!consolidatedIndexRows.ContainsKey(itemKey))
                //{
                //    consolidatedIndexRows.Add(itemKey, new List<IndexRow>());
                //    debugDistinctRows.Add(itemKey, 1);
                //}
                //else
                //    debugDistinctRows[itemKey]++;
#endregion
            }

            // debugger.DebugDictionaryRows(debugDistinctRows);

#endregion

#region 3. Now consolidate each dictionary into one indexRow and put it in new IndexRows.

            List<IndexRow> newIndexRows = new List<IndexRow>();

            int indRowCounter = 0;
            int indRowMissing = 0;

            foreach (KeyValuePair<string, List<IndexRow>> consolidatedIndexRow in consolidatedIndexRows)
            {
                if (1 < consolidatedIndexRow.Value.Count)
                {
                    double weight = 0;
                    double rateOfReturn = 0;

                    foreach (IndexRow indexRow in consolidatedIndexRow.Value)
                        weight += indexRow.Weight;

                    foreach (IndexRow indexRow in consolidatedIndexRow.Value)
                        rateOfReturn += weight == 0 ? 0 : indexRow.RateOfReturn * indexRow.Weight / weight;

                    consolidatedIndexRow.Value[0].Weight = weight;
                    consolidatedIndexRow.Value[0].RateOfReturn = rateOfReturn;
                }

                if (consolidatedIndexRow.Value.Count > 0)
                    newIndexRows.Add(consolidatedIndexRow.Value[0]);
                else
                    ConsoleWriter.cWriteError("consolidatedIndexRow.Value = null!!! for key:[ " +
                        (indRowMissing++).ToString() + " of " + indRowCounter.ToString() + "] " +
                         consolidatedIndexRow.Key.Replace('\a', ' '));

                indRowCounter++;
            }

#endregion

            // 4. Return the consolidated newIndexRows.
            return newIndexRows;
        }



#endif // commented

    }
}
