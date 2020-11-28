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
using LumenWorks.Framework.IO.Csv;

namespace IndexDataEngineLibrary 
{
    public sealed class RussellIcbData
    {
        #region privates, enums, constants
        private StreamWriter swLogFile = null;
        private string LogFileName;
        private SqlConnection mSqlConn = null;
        private SqlDataReader mSqlDr = null;
        private int ConstituentCount = 0;
        private string mPrevId;

        private CultureInfo mCultureInfo = new CultureInfo("en-US");

        private List<IndexRow> indexRowsTickerSort = new List<IndexRow>();
        private List<IndexRow> indexRowsIndustrySort = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel1RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel2RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel3RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel4RollUp = new List<IndexRow>();

        private const string NumberFormat = "0.#########";

        private SharedData sharedData = null;

        private enum VendorFileFormats
        {
            H_OPEN_ICB,
            H_CLOSE_ICB,
            ALL
        }

        private const string TOTAL_COUNT = "Total Count:";
        private bool logReturnData = false;

        #endregion End privates, enums, constants

        #region Production Code
        /***********************/

        #region Constructor / Finish 

        public RussellIcbData()
        {
            sharedData = new SharedData();
            DateHelper.ConnectionString = sharedData.ConnectionStringAmdVifs;
            sharedData.Vendor = Vendors.RussellIcb;
        }

        private void OpenLogFile()
        {
            LogFileName = AppSettings.Get<string>("RussellLogFile");
            if(swLogFile == null)
            {
                if(!File.Exists(LogFileName))
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
            if(swLogFile != null)
            {
                //swLogFile.Flush();
                swLogFile.Close();
                swLogFile = null;
            }
        }



        private void CloseGlobals()
        {
            if(mSqlDr != null)
            {
                mSqlDr.Close();
                mSqlDr = null;
            }
            if(mSqlConn != null)
            {
                mSqlConn.Close();
                mSqlConn = null;
            }
            mPrevId = "";

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
                for(int i = 0; i < Indices.Length; i++)
                {
                    IndexName = Indices[i];
                    if(ProcessStatus.GenerateReturns(sProcessDate, Vendors.RussellIcb.ToString(), Dataset, IndexName))
                    {
                        GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Constituent, false);
                        ProcessStatus.Update(sProcessDate, Vendors.RussellIcb.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlConstituentData, ProcessStatus.StatusValue.Pass);
                        GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Sector, false);
                        ProcessStatus.Update(sProcessDate, Vendors.RussellIcb.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlSectorData, ProcessStatus.StatusValue.Pass);
                        sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.RussellIcb, Dataset, IndexName, AdventOutputType.Constituent);
                        sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.RussellIcb, Dataset, IndexName, AdventOutputType.Sector);
                    }
                }
                sharedData.VendorDatasetJobsUpdateProcessDate(Vendors.RussellIcb.ToString(), Dataset, sProcessDate);
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: VendorDatasetJobs complete  " + Vendors.RussellIcb.ToString() + " " + Dataset + " " + sProcessDate);
                IndexDataEngine indexDataEngine = new IndexDataEngine();
                indexDataEngine.ProcessSecurityMasterReport(sProcessDate);
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                    FROM         RussellIcbDailyTotals
                    WHERE     (FileType = 'open')
                    ORDER BY FileDate desc
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    if(dr.Read())
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
                    FROM         RussellIcbDailyTotals
                    WHERE     (FileType = 'close')
                    ORDER BY FileDate desc
                    ";
                cmd.CommandText = SqlSelect;
                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        sFileDate = dr["FileDate"].ToString();
                        CloseFileDate = DateTime.Parse(sFileDate);
                        CloseVendorTotal = (int)dr["VendorTotal"];
                        CloseAdventTotal = (int)dr["AdventTotal"];
                    }
                }
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
            if(!bUpdated)
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
            string FilePath = AppSettings.Get<string>("VifsPath.RussellIcb");
            string FileName;
            string sMsg = "ProcessVendorFiles: ";

            LogHelper.WriteLine(sMsg + "Started " + DateTime.Now);

            try
            {
                for(oProcessDate = oStartDate
                   ; (DateCompare = oProcessDate.CompareTo(oEndDate)) <= 0
                   ; oProcessDate = oProcessDate.AddDays(1))
                {
                    if(bOpenFiles)
                    {
                        FileName = FilePath + "H_OPEN_R3000E_" + oProcessDate.ToString("yyyyMMdd") + ".csv";
                        if(File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            if(bOpenFiles)
                            {
                                AddRussellOpeningData(VendorFileFormats.H_OPEN_ICB, FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.RussellIcb.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                                ProcessStatus.Update(oProcessDate, Vendors.RussellIcb.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);

                            }
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }

                    if(bCloseFiles)
                    {
                        FileName = FilePath + "H_" + oProcessDate.ToString("yyyyMMdd") + "_R3000E.csv";
                        if(File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            if(bCloseFiles)
                            {
                                AddRussellClosingData(VendorFileFormats.H_CLOSE_ICB, FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.RussellIcb.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }
                    if(bTotalReturnFiles)
                    {
                        FileName = FilePath + "ALL" + oProcessDate.ToString("yyyyMMdd") + ".csv";
                        if(File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            AddRussellTotalReturnData(VendorFileFormats.ALL, FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.RussellIcb.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }

                    if(bSymbolChanges)
                    {
                        FileName = FilePath + "Updates_" + oProcessDate.ToString("yyyyMMdd") + "_R3000E.csv";
                        if(File.Exists(FileName))
                        {
                            LogHelper.WriteLine("Processing: " + FileName + " " + DateTime.Now);
                            if(bSymbolChanges)
                            {
                                AddRussellSymbolChangeData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.RussellIcb.ToString(), Dataset, "", ProcessStatus.WhichStatus.SymbolChangeData, ProcessStatus.StatusValue.Pass);
#if Commented

                                AddRussellSecurityReferenceChangeData(FileName, oProcessDate);
#endif
                            }
                            LogHelper.WriteLine("Done      : " + FileName + " " + DateTime.Now);
                        }
                    }

                }
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
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
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            try
            {
                string sFileType = "";
                switch(FileFormat)
                {
                    case VendorFileFormats.H_CLOSE_ICB:
                        sFileType = "Close";
                        break;
                    case VendorFileFormats.H_OPEN_ICB:
                        sFileType = "Open";
                        break;
                }
                cnSql.Open();
                string SqlSelect = @"
                    select count(*) from RussellIcbDailyTotals
                    where FileType = @FileType
                    and FileDate = @FileDate
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@FileType", SqlDbType.VarChar);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                cmd.Parameters["@FileType"].Value = sFileType;
                cmd.Parameters["@FileDate"].Value = FileDate;
                int iCount = (int)cmd.ExecuteScalar();
                if(iCount == 1)
                {
                    cmd.CommandText = @"
                        delete from RussellIcbDailyTotals
                        where FileType = @FileType
                        and FileDate = @FileDate
                        ";
                    cmd.ExecuteNonQuery();
                }
                cmd.CommandText = @"
                    insert into RussellIcbDailyTotals (FileType, FileDate, FileName, VendorTotal, AdventTotal, ZeroSharesTotal)
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
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
            if(string.IsNullOrEmpty(number))
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
                if(number.Equals("#N/A", StringComparison.OrdinalIgnoreCase) || number == "-" || number.StartsWith("..."))
                    return 0;
                else if(number.StartsWith("(", StringComparison.Ordinal) &&
                    number.EndsWith(")", StringComparison.Ordinal) && 3 <= number.Length)
                {
                    try { return Convert.ToDouble("-" + number.Substring(1, number.Length - 2), mCultureInfo); }
                    catch { return NullDouble; }
                }
                else
                    return NullDouble;
            }
        }

        private void AddRussellSymbolChangeData(string FileName, DateTime FileDate)
        {
            DataTable dt = ReadCsvIntoTable(FileName);
            int i = 0;

            try
            {
                // Look at the Symbol Changes for the previous date
                DateTime CompareDate = DateHelper.PrevBusinessDay(FileDate);
                foreach(DataRow dr in dt.Rows)
                {
                    i += 1;

                    DateTime oDate = DateTime.MinValue;
                    CultureInfo enUS = new CultureInfo("en-US");
                    string sOldCusip = "";
                    string sNewCusip = "";
                    string sOldTicker = "";
                    string sNewTicker = "";
                    string sOldCompanyName = "";
                    string sNewCompanyName = "";
                    string sCompanyName = "";

                    string Fld = ParseColumn(dr, "DATE", 1);
                    if(Fld.Length == 10 && DateTime.TryParseExact(Fld, "MM/dd/yyyy", enUS, DateTimeStyles.None, out oDate))
                    {
                        if(oDate.Equals(CompareDate))
                        {
                            Fld = ParseColumn(dr, "CUR CUSIP", 4);
                            if(Fld.Length == 9)
                            {
                                Fld = Fld.Substring(0, 8);
                                sOldCusip = Fld;
                            }
                            else
                                sOldCusip = "";

                            Fld = ParseColumn(dr, "NEW CUSIP", 5);
                            if(Fld.Length == 9)
                            {
                                Fld = Fld.Substring(0, 8);
                                sNewCusip = Fld;
                            }
                            else
                                sNewCusip = "";

                            sOldTicker = ParseColumn(dr, "CUR TICKER", 8);
                            sNewTicker = ParseColumn(dr, "NEW TICKER", 9);

                            sOldCompanyName = ParseColumn(dr, "CUR COMPANY NAME", 10);
                            sNewCompanyName = ParseColumn(dr, "NEW COMPANY NAME", 11);
                            if(sNewCompanyName.Length > 0)
                                sCompanyName = sNewCompanyName;
                            else
                                sCompanyName = sOldCompanyName;

                            if(sOldCusip.Length == 8 && sNewCusip.Length == 8)
                                sharedData.AddSymbolChange("4", FileDate, sOldCusip, sNewCusip, sCompanyName);
                            if(sOldTicker.Length > 0 && sNewTicker.Length > 0)
                                sharedData.AddSymbolChange("4", FileDate, sOldTicker, sNewTicker, sCompanyName);
                        }
                    }
                }
            }
            catch
            {
                LogHelper.WriteLine("AddRussellSymbolChangeData " + FileName + " error line: " + i);
            }
            finally
            {

            }
        }


#if Commented
        private void AddRussellSecurityReferenceChangeData(string FileName, DateTime FileDate)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;


            DataTable dt = ReadCsvIntoTable(FileName);
            int i = 0;

            try
            {
                SqlDelete = "delete FROM HistoricalSecurityMasterFullChanges ";
                SqlWhere = "where FileDate = @FileDate";
                cmd = new SqlCommand();
                cmd.Connection = cnSql;
                cmd.CommandText = SqlDelete + SqlWhere;
                cmd.Parameters.Add("@FileDate", SqlDbType.Date);
                cmd.Parameters["@FileDate"].Value = FileDate;
                cmd.ExecuteNonQuery();

                foreach(DataRow dr in dt.Rows)
                {
                    i += 1;

                    DateTime oDate = DateTime.MinValue;
                    CultureInfo enUS = new CultureInfo("en-US");
                    string sOldCusip = "";
                    string sNewCusip = "";
                    string sOldTicker = "";
                    string sNewTicker = "";
                    string sOldCompanyName = "";
                    string sNewCompanyName = "";
                    string sOldExchange = "";
                    string sNewExchange = "";
                    string sOldIndustry = "";
                    string sNewIndustry = "";
                    string sTermsDetail = "";
                    string sAction = "";
                    string sReason = "";

                    string Fld = ParseColumn(dr, "DATE", 1);
                    if(Fld.Length == 10 && DateTime.TryParseExact(Fld, "MM/dd/yyyy", enUS, DateTimeStyles.None, out oDate))
                    {
                        if(oDate.Equals(FileDate))
                        {
                            sAction = ParseColumn(dr, "ACTION", 2);
                            sReason = ParseColumn(dr, "REASON", 3);

                            Fld = ParseColumn(dr, "CUR CUSIP", 4);
                            if(Fld.Length == 9)
                            {
                                Fld = Fld.Substring(0, 8);
                                sOldCusip = Fld;
                            }
                            else
                                sOldCusip = "";

                            Fld = ParseColumn(dr, "NEW CUSIP", 5);
                            if(Fld.Length == 9)
                            {
                                Fld = Fld.Substring(0, 8);
                                sNewCusip = Fld;
                            }
                            else
                                sNewCusip = "";

                            sOldTicker = ParseColumn(dr, "CUR TICKER", 8);
                            sNewTicker = ParseColumn(dr, "NEW TICKER", 9);

                            sOldCompanyName = ParseColumn(dr, "CUR COMPANY NAME", 10);
                            sNewCompanyName = ParseColumn(dr, "NEW COMPANY NAME", 11);

                            sOldIndustry = ParseColumn(dr, "CUR SUBSECTOR", 43);                   
                            sNewIndustry = ParseColumn(dr, "NEW SUBSECTOR", 44);

                            sOldExchange = ParseColumn(dr, "CUR EXCHANGE", 45);
                            sNewExchange = ParseColumn(dr, "NEW EXCHANGE", 46);

                            sTermsDetail = ParseColumn(dr, "TERMS DETAIL", 47);


                            if( sNewCusip.Length > 0 || sNewTicker.Length > 0 || sNewCompanyName.Length > 0 || sNewIndustry.Length > 0 || sNewExchange.Length > 0 )
                            {
                                sharedData.AddSecRefChangeData( FileDate, sOldCusip, sNewCusip, sOldTicker, sNewTicker, sOldCompanyName, sNewCompanyName,
                                                                sOldExchange, sNewExchange, sOldIndustry, sNewIndustry);                                    
                            }
                        }
                    }
                }
            }
            catch
            {
                LogHelper.WriteLine("AddRussellSecurityReferenceChangeData " + FileName + " error line: " + i);
            }
            finally
            {

            }
        }
#endif


        private void AddRussellTotalReturnData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {

            string[] sIndices = GetVendorIndices();
            //foreach (string sIndex in sIndices)
            //{
            //    DeleteTotalReturn(FileDate, sIndex);
            //}

            DataTable dt = ReadCsvIntoTable(FileName);
            int i = 0;

            foreach(DataRow dr in dt.Rows)
            {
                i += 1;

                bool ok = true;

                string Fld = ParseColumn(dr, "INDEX", 0); 
                if(i == 1)
                    ok = false;
                else
                {
                    ok = true;

                    if(Fld.StartsWith("XX")) // EOF
                        ok = false;
                    else if(Fld.StartsWith("R"))
                    {
                        string sVendorIndex = "";
                        string sAdventIndex = "";
                        string sCurrency = "";
                        string sTotal = "";

                        sCurrency = ParseColumn(dr, "CRNY", 2);
                        if(sCurrency.Equals("USD"))
                        {
                            sVendorIndex = ParseColumn(dr, "INDEX", 0);
                            sTotal = ParseColumn(dr, "TOTAL", 3);

                            if(sIndices.Contains(sVendorIndex))
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
        }

        void csv_ParseError(object sender, ParseErrorEventArgs e)
        {
            //if (e.Error is MissingFieldCsvException)
            //LogHelper.WriteLine("APUtilities: CsvParseError: " + DateTime.Now);
            //LogHelper.WriteLine("         CurrentFieldIndex: " + e.Error.CurrentFieldIndex);
            //LogHelper.WriteLine("           CurrentPosition: " + e.Error.CurrentPosition);
            //LogHelper.WriteLine("        CurrentRecordIndex: " + e.Error.CurrentRecordIndex);
            //LogHelper.WriteLine("                   Message: " + e.Error.Message);
            //LogHelper.WriteLine("                   RawDate: " + e.Error.RawData);
            //LogHelper.WriteLine("--------------------------: " );

            e.Action = ParseErrorAction.AdvanceToNextLine;
        }


        private DataTable ReadCsvIntoTable(string Filename)
        {
            DataTable dt = new DataTable();
            try
            {
                bool HasHeaders = false;
                using(CsvReader csv =
                           new CsvReader(new StreamReader(Filename), HasHeaders))
                {
                    csv.DefaultParseErrorAction = ParseErrorAction.RaiseEvent;
                    csv.ParseError += csv_ParseError;
                    dt.Load(csv);
                }

                // // Old read that didn't work cuz of # comment character which is ok
                //using (CsvReader csv =
                //           new CsvReader(new StreamReader(Filename), HasHeaders, '\t'))
                //{
                //    csv.DefaultParseErrorAction = ParseErrorAction.RaiseEvent;
                //    csv.ParseError += csv_ParseError;
                //    dt.Load(csv);
                //}
            }
            catch
            {
            }
            finally
            {
            }
            return (dt);
        }

        private string ParseColumn(DataRow dr, string column)
        {
            string value = "";
            if(dr.Table.Columns.Contains(column))
            {
                if(!dr.IsNull(column))
                    value = dr[column].ToString();
            }
            return (value);
        }

        private string ParseColumn(DataRow dr, string column, int index)
        {
            string value = "";

            value = dr[index].ToString();

            //if(dr.Table.Columns.Contains(column))
            //{
            //    if(!dr.IsNull(column))
            //        value = dr[column].ToString();
            //}
            return (value);
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

            cnSql1.Open();
            cnSql2.Open();

            SqlDelete = "delete FROM RussellIcbDailyHoldings1 ";
            SqlWhere = "where FileDate = @FileDate";
            cmdHoldings = new SqlCommand();
            cmdHoldings.Connection = cnSql1;
            cmdHoldings.CommandText = SqlDelete + SqlWhere;
            cmdHoldings.Parameters.Add("@FileDate", SqlDbType.Date);
            cmdHoldings.Parameters["@FileDate"].Value = FileDate;
            cmdHoldings.ExecuteNonQuery();
            SqlDelete = "delete FROM RussellIcbDailyHoldings2 ";
            cmdHoldings.CommandText = SqlDelete + SqlWhere;
            cmdHoldings.ExecuteNonQuery();
            SqlDelete = "delete FROM HistoricalSecurityMasterFullChanges ";
            SqlWhere = "where ProcessDate = @FileDate";
            cmdHoldings.CommandText = SqlDelete + SqlWhere;
            cmdHoldings.ExecuteNonQuery();

            string sTable1 = "RussellIcbDailyHoldings1";
            string sTable2 = "RussellIcbDailyHoldings2";

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

            DataTable dt = ReadCsvIntoTable(FileName);
            int i = 0;

            // Date Cons Code Reserved    Cusip ISIN    Ticker Exchange    Constituent Name    Industry Code   Supersector Code    Sector Code Subsector Code  Cash Dividend   ShareChg Shares  Reserved Investability Weight Value Probability Growth Probability Defensive Probability Dynamic Probability Return  MTD Return  Mkt Cap(USD) after investability weight Reserved    R1000 R2000   R2500 RMID    RTOP200 RSCC    RTOP50 R3000   RMICRO WT-R3E  WT - R3EG WT - R3EV WT - R1   WT - R1G  WT - R1V  WT - R2   WT - R2G  WT - R2V  WT - R25  WT - R25G WT - R25V WT - RMID WT - MIDG WT - MIDV WT - RT2  WT - RT2G WT - RT2V WT - RSSC WT - RSSCG    WT - RSSCV    WT - R3   WT - R3G  WT - R3V  WT - MICRO    WT - MICROG   WT - MICROV

            foreach(DataRow dr in dt.Rows)
            {
                i += 1;

                bool ok = true;

                string sCUSIP = "";
                string sTicker = "";
                string sCompanyName = "";
                string sSector = "";
                string sExchange = "";
                string sStockKey = "";

                if(i == 1)
                    ok = false;
                else
                {
                    Fld = ParseColumn(dr, "Date", 0);  // 0
                    if(Fld.StartsWith("XX")) // EOF
                        ok = false;
                    else
                        ok = true;
                }

                if(ok)
                {
                    drHoldings1 = dtHoldings1.NewRow();

                    Fld = ParseColumn(dr, "Date", 0);  // 0
                    if( Fld.Length == 8 && DateTime.TryParseExact(Fld, "yyyyMMdd", enUS, DateTimeStyles.None, out oDate))
                    {
                        Fld = oDate.ToString("MM/dd/yyyy");
                        drHoldings1["FileDate"] = Fld;
                    }
                    else
                        ok = false;

                    Fld = ParseColumn(dr, "Cusip", 3); // 3
                    if(Fld.Length == 9)
                    {
                        Fld = Fld.Substring(0, 8);
                        drHoldings1["CUSIP"] = Fld;
                        sCUSIP = Fld;
                    }
                    else
                        ok = false;

                    Fld = ParseColumn(dr, "Ticker", 5);    // 5        
                    if(Fld.Length >= 1)
                    {
                        drHoldings1["Ticker"] = Fld;
                        sTicker = Fld;
                    }
                    else
                        ok = false;
                    //LogHelper.WriteLine(sTicker + " " + SecurityCount);

                    Fld = ParseColumn(dr, "Mkt Cap (USD) after investability weight", 23); // 23
                    drHoldings1["MktValue"] = Fld;

                    Fld = ParseColumn(dr, "Shares", 14); // 14
                    double dShares = Convert.ToDouble(Fld);
                    drHoldings1["SharesDenominator"] = Fld;
                    if(Convert.ToInt64(Fld.ToString()) == 0)
                        SharesDenominatorZeroCount += 1;

                    DateTime newFormatDate = DateTime.ParseExact("07/28/2020", "MM/dd/yyyy", CultureInfo.InvariantCulture);


                    if(FileDate >= newFormatDate)
                    {
                        Fld = ParseColumn(dr, "Value Shares", 17); // 17
                        if(Fld.Length > 0)
                            sSharesValue = Fld;
                        else
                            sSharesValue = "0";

                        Fld = ParseColumn(dr, "Growth Shares", 18); // 18
                        if(Fld.Length > 0)
                            sSharesGrowth = Fld;
                        else
                            sSharesGrowth = "0";
                    }
                    else
                    {
                        double dMultiplier = 0;
                        // value and growth are multipliers rather than an amount
                        Fld = ParseColumn(dr, "Value Probability", 17); // 17
                        if(Fld.Length > 0 && !Fld.Equals("0"))
                        {
                            dMultiplier = Convert.ToDouble(Fld);
                            double dValueShares = dShares * dMultiplier;
                            dValueShares = Math.Round(dValueShares, MidpointRounding.AwayFromZero);
                            sSharesValue = Convert.ToString(Convert.ToInt64(dValueShares));
                        }
                        else
                            sSharesValue = "0";

                        Fld = ParseColumn(dr, "Growth Probability", 18); // 18
                        if(Fld.Length > 0 && !Fld.Equals("0"))
                        {
                            dMultiplier = Convert.ToDouble(Fld);
                            double dGrowthShares = dShares * dMultiplier;
                            dGrowthShares = Math.Round(dGrowthShares, MidpointRounding.AwayFromZero);
                            sSharesGrowth = Convert.ToString(Convert.ToInt64(dGrowthShares));
                        }
                        else
                            sSharesGrowth = "0";
                    }
                    r1000 = ParseColumn(dr, "R1000", 25); // 25
                    r2000 = ParseColumn(dr, "R2000", 26); // 26
                    r2500 = ParseColumn(dr, "R2500", 27); // 27
                    rmid = ParseColumn(dr, "RMID", 28);   // 28
                    rtop200 = ParseColumn(dr, "RTOP200", 29); // 29
                    rsmallc = ParseColumn(dr, "RSCC", 30); // 30
                    r3000 = ParseColumn(dr, "R3000", 32);  // 32
                    rmicro = ParseColumn(dr, "RMICRO", 33);// 33

                    Fld = ParseColumn(dr, "Constituent Name", 7); // 7
                    sCompanyName = Fld;

                    Fld = ParseColumn(dr, "Subsector Code", 11); // 11
                    drHoldings1["Sector"] = Fld;
                    sSector = Fld;

                    Fld = ParseColumn(dr, "Exchange", 6); // 6
                    sExchange = Fld;

                    Fld = ParseColumn(dr, "Cons Code", 1); // 1
                    sStockKey = Fld;

                    if(ok)
                    {
                        sharedData.AddSecurityMasterFull(sStockKey, sTicker, sCUSIP, "4", sCompanyName, sSector, sExchange, oDate);                  
                        if(r1000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r1000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r1000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r1000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(r2000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r2000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r2000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r2000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(r2500.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r2500";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r2500g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r2500v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(rmid.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rmid";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rmidg";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rmidv";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(rtop200.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rtop200";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rtop200g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rtop200v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(rsmallc.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rsmallc";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rsmallcg";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "rsmallcv";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(r3000.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "r3000";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "r3000g";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesValue;
                                drHoldings2["IndexName"] = "r3000v";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                        }
                        if(rmicro.Equals("Y"))
                        {
                            drHoldings2 = dtHoldings2.NewRow();
                            drHoldings2["FileDate"] = drHoldings1["FileDate"];
                            drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                            drHoldings2["SharesNumerator"] = drHoldings1["SharesDenominator"];
                            drHoldings2["IndexName"] = "rmicro";
                            dtHoldings2.Rows.Add(drHoldings2);
                            if(Convert.ToInt64(sSharesGrowth) > 0)
                            {
                                drHoldings2 = dtHoldings2.NewRow();
                                drHoldings2["FileDate"] = drHoldings1["FileDate"];
                                drHoldings2["CUSIP"] = drHoldings1["CUSIP"];
                                drHoldings2["SharesNumerator"] = sSharesGrowth;
                                drHoldings2["IndexName"] = "rmicrog";
                                dtHoldings2.Rows.Add(drHoldings2);
                            }
                            if(Convert.ToInt64(sSharesValue) > 0)
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

                        }
                        catch(SqlException ex)
                        {
                            if(ex.Number == 2627)
                            {
                                LogHelper.WriteLine(ex.Message);
                            }
                        }
                        finally
                        {
                        }
                    }
                }
            }
            try
            {
                daAdvIndexData1.Update(dtHoldings1);
                daAdvIndexData2.Update(dtHoldings2);
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }
        }



        private void AddRussellClosingData(VendorFileFormats FileFormat, string FileName, DateTime FileDate)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            cnSql.Open();
            string sTable = "RussellIcbDailyHoldings1";
            string SqlUpdate = "update " + sTable + " set SecurityReturn = @SecurityReturn ";
            string SqlWhere = "where FileDate = @FileDate and CUSIP = @CUSIP ";
            SqlCommand cmdHoldingsRec = new SqlCommand(SqlUpdate + SqlWhere, cnSql);
            cmdHoldingsRec.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
            cmdHoldingsRec.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
            cmdHoldingsRec.Parameters.Add("@SecurityReturn", SqlDbType.VarChar, 7);

            CultureInfo enUS = new CultureInfo("en-US");

            DateTime oDate = DateTime.MinValue;
            string Fld;
            string sFileDate = "";
            string sCUSIP = "";
            string sSecurityReturn = "";
            string sShares = "";
            int SecurityCount = 0;
            int SharesDenominatorZeroCount = 0;

            DataTable dt = ReadCsvIntoTable(FileName);
            int i = 0;

            foreach(DataRow dr in dt.Rows)
            {
                i += 1;

                bool ok = true;

                Fld = ParseColumn(dr, "Date", 0);  // 0
                if(i == 1)
                    ok = false;
                else
                {
                    if(Fld.StartsWith("XX")) // EOF
                        ok = false;
                    else
                        ok = true;
                }

                if(ok)
                {
                    Fld = ParseColumn(dr, "Date", 0);  // 0
                    if(Fld.Length == 8 && DateTime.TryParseExact(Fld, "yyyyMMdd", enUS, DateTimeStyles.None, out oDate))
                    {
                        Fld = oDate.ToString("MM/dd/yyyy");
                        sFileDate = oDate.ToString("MM/dd/yyyy");
                    }
                    else
                        ok = false;


                    Fld = ParseColumn(dr, "Cusip", 3); // 3
                    if(Fld.Length == 9)
                    {
                        Fld = Fld.Substring(0, 8);
                        sCUSIP = Fld;
                    }
                    else
                        ok = false;

                    Fld = ParseColumn(dr, "Return", 21);
                    sSecurityReturn = Fld;

                    Fld = ParseColumn(dr, "Shares", 10);
                    sShares = Fld;
                    if(Convert.ToInt64(sShares.ToString()) == 0)
                        SharesDenominatorZeroCount += 1;
                }

                if(ok)
                {
                    SecurityCount += 1;
                    try
                    {
                        if(GetDailyHoldings1Count(oDate, sCUSIP) == 0)
                        {
                            string sOldCUSIP = GetOldCUSIP(sCUSIP);
                            if(sOldCUSIP.Length == 8)
                            {
                                if(GetDailyHoldings1Count(oDate, sOldCUSIP) == 0)
                                {
                                    LogHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString() +
                                                        " linked to Old CUSIP " + sOldCUSIP.ToString());
                                    ok = false;
                                }

                                //throw new Exception("AddRussellClosingData can't update " + sCUSIP + " for date" + oDate.ToShortDateString());
                                else
                                    sCUSIP = sOldCUSIP;
                            }
                            else if(sOldCUSIP.Length == 0)
                            {
                                LogHelper.WriteLine("Skipping line:" + sCUSIP.ToString());
                                LogHelper.WriteLine("Can't find opening holdings1 for " + sCUSIP.ToString());
                                ok = false;
                            }

                        }
                        if(ok)
                        {
                            cmdHoldingsRec.Parameters["@CUSIP"].Value = sCUSIP;
                            cmdHoldingsRec.Parameters["@FileDate"].Value = oDate;
                            cmdHoldingsRec.Parameters["@SecurityReturn"].Value = sSecurityReturn;
                            cmdHoldingsRec.ExecuteNonQuery();
                        }
                    }

                    catch(SqlException ex)
                    {
                        if(ex.Number == 2627)
                        {
                            LogHelper.WriteLine(ex.Message);
                            LogHelper.WriteLine(FileName + ":" + sCUSIP);
                        }
                    }
                    finally
                    {
                    }
                }
                if(!ok)
                {
                    LogHelper.WriteLine("Skipping line:" + Fld);
                }
            }
        }

        public int GetDailyHoldings1Count(DateTime FileDate, string sCUSIP)
        {
            SqlConnection conn = new SqlConnection(sharedData.ConnectionStringIndexData);
            int Count = 0;
            try
            {
                string SqlSelectCount = @"
                    select count(CUSIP) from RussellIcbDailyHoldings1 where FileDate = @FileDate and CUSIP = @CUSIP 
                    ";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount, conn);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
                cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
                cmd.Parameters["@CUSIP"].Value = sCUSIP;
                cmd.Parameters["@FileDate"].Value = FileDate;
                Count = (int)cmd.ExecuteScalar();
            }
            catch(SqlException ex)
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
                    SELECT oldSymbol FROM HistoricalSymbolChanges WHERE newSymbol = @newSymbol
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@newSymbol", SqlDbType.VarChar, 8);
                cmd.Parameters["@newSymbol"].Value = sNewCUSIP;
                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        sOldCUSIP = dr["oldSymbol"].ToString();
                    }
                }

            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                if(mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                string SqlSelect = @"
                    select SecurityReturn from  RussellIcbDailyHoldings1
                    where FileDate = @FileDate and CUSIP = @CUSIP
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@FileDate", SqlDbType.DateTime, 8);
                cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar, 8);
                cmd.Parameters["@FileDate"].Value = FileDate;
                cmd.Parameters["@CUSIP"].Value = CUSIP;
                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        sSecurityReturn = dr["SecurityReturn"].ToString();
                    }
                }
                else
                {
                    string msg = "no row found RussellIcbDailyHoldings1: " + FileDate.ToShortDateString() + "CUSIP: " + CUSIP;
                    //ConsoleWriter.cWriteInfo(msg);
                }
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                if(mSqlConn == null)
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
                if(count > 0)
                {
                    // todo-jk ConsoleWriter.cWriteInfo("Generating " + count + " Historical Security Master files. ");
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    SqlDataReader dr = cmd.ExecuteReader();
                    int i = 0;
                    while(dr.Read())
                    {
                        i += 1;

                        string LogFileName = GetLogFileName();
                        CloseLogFile();
                        if(File.Exists(LogFileName))
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
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
            try
            {
                SqlConnection SqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                SqlConn.Open();

                string SqlSelect = null;

                if(sVendor.Equals("StandardAndPoors"))
                {
                    SqlSelect = @"
                        SELECT  sm.Ticker, sm.Cusip, sm.Description, sm.Vendor, sm.IndexId, sm.BeginDate, sm.EndDate, 
                                sc.SectorCode, sc.SectorDesc, sc.IndustryGroupCode, sc.IndustryGroupDesc, 
                                sc.IndustryCode, sc.IndustryDesc, sc.SubIndustryCode, sc.SubIndustryDesc
                        FROM    HistoricalSecurityMasterFull sm 
                        INNER JOIN
                                SectorCodesGICS sc ON sm.IndustryGroup = sc.SubIndustryCode ";
                }
                else if(sVendor.Equals("Russell"))
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
                while(dr.Read())
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
                    if(sVendor.Equals("StandardAndPoors"))
                    {
                        sLine +=
                            "\"" + dr["SectorCode"].ToString() + "\"," + "\"" + dr["SectorDesc"].ToString() + "\"," +
                            "\"" + dr["IndustryGroupCode"].ToString() + "\"," + "\"" + dr["IndustryGroupDesc"].ToString() + "\"," +
                            "\"" + dr["IndustryCode"].ToString() + "\"," + "\"" + dr["IndustryDesc"].ToString() + "\"," +
                            "\"" + dr["SubIndustryCode"].ToString() + "\"," + "\"" + dr["SubIndustryDesc"].ToString() + "\"";

                    }
                    else if(sVendor.Equals("Russell"))
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
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
            try
            {
                if(mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from RussellIcbDailyIndexReturns
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
                if(iCount == 0)
                {
                    cmd.CommandText =
                        "insert into RussellIcbDailyIndexReturns (IndexName, FileDate, TotalReturn) " +
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
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
            }

        }

#endregion End Total Return

        /*******************************/
#endregion  End Production Code


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
                string SqlWhere = "where Vendor = 'RussellICB' and Supported = 'Yes' and IndexName ='" + VendorIndex + "'";
                //                string SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' and VendorIndexName ='" + VendorIndex + "'";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere, conn);
                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    dr.Read();
                    //AdventIndex = dr["AdventIndexName"].ToString();
                    AdventIndex = dr["IndexClientName"].ToString();
                }
            }
            catch(SqlException ex)
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
            sharedData.Vendor = Vendors.RussellIcb;
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
                string SqlWhere = "where Vendor = 'RussellICB' and Supported = 'Yes' ";
                //string SqlOrderBy = "order by VendorIndexName";
                string SqlOrderBy = "order by IndexName";

                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if(count > 0)
                {
                    Indices = new string[count];
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while(dr.Read())
                    {
                        //Indices[i] = dr["VendorIndexName"].ToString();
                        Indices[i] = dr["IndexName"].ToString();
                        i += 1;
                    }
                }
            }
            catch(SqlException ex)
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
                SqlSelect = "select count (distinct FileDate) from RussellIcbDailyHoldings1 ";
                SqlWhere = "where FileDate >= '" + sStartDate + "' ";
                SqlWhere += "and FileDate <= '" + sEndDate + "' ";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if(count > 0)
                {
                    IndexReturnArray = new IndexReturnStruct[count];
                    SqlSelect = "select distinct FileDate from RussellIcbDailyHoldings1 ";
                    cmd.CommandText = SqlSelect + SqlWhere;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while(dr.Read())
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
                    for(i = 0; i < IndexReturnArray.Length; i++)
                    {
                        dProduct *= IndexReturnArray[i].IndexReturn2;
                    }
                    //dProduct = dProduct;
                    double dRetForPeriod = (dProduct - 1) * 100;
                    //LogHelper.WriteLine("Return for period " + sStartDate + " to " + sEndDate + " for " + sIndexName + " = " + dRetForPeriod);
                }
            }

            catch(SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                if(dr != null)
                    dr.Close();
                conn.Close();
                LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                //LogHelper.Flush();
            }

            return;
        }

        public bool LogReturnData
        {
            get { return logReturnData; }
            set { logReturnData = value; }
        }


        private void AdjustReturnsToMatchPublishedTotalReturns(List<IndexRow> indexRows, string sDate, string sIndexName, string sVendorFormat)
        {
            if(logReturnData)
            {
                LogHelper.WriteLine("----------------------------------------------------------------------------------------------");
                LogHelper.WriteLine("Total Return is the sum of: mWeight * mRateOfReturn * .01");
                LogHelper.WriteLine("AdjustReturnsToMatchPublishedTotalReturns " + sDate + " " + sIndexName + " " + sVendorFormat);
            }

            int totalReturnPrecision = 9;
            double dZero = 0.0;


            double VendorTotalReturn = GetVendorTotalReturnForDate(sDate, sIndexName);

            if(VendorTotalReturn.Equals(dZero) == false)
            {

                VendorTotalReturn = Math.Round(VendorTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);
                if(logReturnData)
                    LogHelper.WriteLine("Vendor Total Return " + VendorTotalReturn.ToString());

                IndexRows.ZeroAdventTotalReturn();
                // Advent Total Return is the sum of: mWeight * mRateOfReturn * .01;
                foreach(IndexRow indexRow in indexRows)
                    indexRow.CalculateAdventTotalReturn();

                double AdventTotalReturn = IndexRows.AdventTotalReturn;
                AdventTotalReturn = Math.Round(AdventTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);
                if(logReturnData)
                    LogHelper.WriteLine("Advent Total Return " + AdventTotalReturn.ToString());

                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.RussellIcb.ToString(), sVendorFormat, AdventTotalReturn, "AdvReturn");

                double AdventVsVendorDiff = VendorTotalReturn - AdventTotalReturn;

                AdventVsVendorDiff = Math.Round(AdventVsVendorDiff, totalReturnPrecision, MidpointRounding.AwayFromZero);
                if(logReturnData)
                    LogHelper.WriteLine("Advent Vs Vendor Diff " + AdventVsVendorDiff.ToString());

                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.RussellIcb.ToString(), sVendorFormat, AdventVsVendorDiff, "Diff");

                IndexRows.CalculateAddlContribution(AdventVsVendorDiff, sVendorFormat, logReturnData);

                LogHelper.WriteLine("Identifier,Weight,RateOfReturn,RateOfReturnAdjustment,RateOfReturnAdjusted");

                foreach(IndexRow indexRow in indexRows)
                {
                    indexRow.CalculateAdventAdjustedReturn();
                    if(logReturnData)
                        LogHelper.WriteLine(indexRow.Identifier + "," + indexRow.Weight.ToString() + "," + indexRow.RateOfReturn.ToString() + "," + indexRow.RateOfReturnAdjustment + "," + indexRow.RateOfReturnAdjusted);
                }


                double AdventTotalReturnAdjusted = IndexRows.AdventTotalReturnAdjusted;
                AdventTotalReturnAdjusted = Math.Round(AdventTotalReturnAdjusted, totalReturnPrecision, MidpointRounding.AwayFromZero);
                if(logReturnData)
                    LogHelper.WriteLine("Advent Total Return Adjusted " + AdventTotalReturnAdjusted.ToString());

                sharedData.AddTotalReturn(sDate, sIndexName, Vendors.RussellIcb.ToString(), sVendorFormat, AdventTotalReturnAdjusted, "AdvReturnAdj");

                if(logReturnData)
                    LogHelper.WriteLine("----------------------------------------------------------------------------------------------");

            }
        }

        private void RollUpRatesOfReturn(List<IndexRow> indexRowsRollUp, List<IndexRow> indexRowsIndustrySort,
            IndexRow.VendorFormat vendorFormat, string sDate, string sIndexName)
        {
            foreach(IndexRow indexRowRollUp in indexRowsRollUp)
                foreach(IndexRow indexRowConstituent in indexRowsIndustrySort)
                {
                    string CompareIndentifier = "";
                    switch(vendorFormat)
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
                    if(CompareIndentifier == indexRowRollUp.Identifier)
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
            indexRowsSectorLevel4RollUp.Clear();

        }


        public void GenerateIndustryReturnsForDate(string sDate, string sIndexName)
        {
            InitializeIndexRows();
            if(GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Sector) == true)
            {
                for(bool GotNext = true; GotNext;)
                {
                    string sCusip = null;
                    string sTicker = null;
                    string sIndustry = null;
                    string sSuperSector = null;
                    string sSector = null;
                    string sSubSector = null;

                    string sWeight = null;
                    string sSecurityReturn = null;

                    if((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out sIndustry, out sSuperSector, out sSector, out sSubSector,
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker,
                                                         sIndustry, sSuperSector, sSector, sSubSector,
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);
                        indexRowsIndustrySort.Add(indexRow);
                    }
                }


                for(IndexRow.VendorFormat vendorFormat = IndexRow.VendorFormat.SECTOR_LEVEL1; vendorFormat <= IndexRow.VendorFormat.SECTOR_LEVEL4; vendorFormat++)
                {
                    string sCurrentIdentifier = "";
                    double rolledUpWeight = 0;
                    foreach(IndexRow indexRow in indexRowsIndustrySort)
                    {
                        if(sCurrentIdentifier.Length == 0)
                        {
                            rolledUpWeight = indexRow.Weight;
                            sCurrentIdentifier = indexRow.GetIdentifier(vendorFormat);
                        }
                        else if(indexRow.GetIdentifier(vendorFormat) == sCurrentIdentifier)
                        {
                            rolledUpWeight += indexRow.Weight;
                        }
                        else if(indexRow.GetIdentifier(vendorFormat) != sCurrentIdentifier)
                        {
                            // finish up and store the current sector's info
                            switch(vendorFormat)
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
                                case IndexRow.VendorFormat.SECTOR_LEVEL4:
                                    IndexRow rollUpIndexRow4 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", "", "", sCurrentIdentifier,
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel4RollUp.Add(rollUpIndexRow4); break;

                            }

                            //... and then move on to start accumulating the next sector's info
                            rolledUpWeight = indexRow.Weight;
                            sCurrentIdentifier = indexRow.GetIdentifier(vendorFormat);
                        }
                        if(indexRowsIndustrySort.Last().Equals(indexRow))
                        {
                            switch(vendorFormat)
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
                                case IndexRow.VendorFormat.SECTOR_LEVEL4:
                                    IndexRow rollUpIndexRow4 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", "", "", sCurrentIdentifier,
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel4RollUp.Add(rollUpIndexRow4); break;

                            }
                        }
                    }

                    switch(vendorFormat)
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
                        case IndexRow.VendorFormat.SECTOR_LEVEL4:
                            RollUpRatesOfReturn(indexRowsSectorLevel4RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            break;
                    }
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

            for(processDate = startDate
            ; (DateCompare = processDate.CompareTo(endDate)) <= 0
            ; processDate = DateHelper.NextBusinessDay(processDate))
            {
                if(isHistoricalAxmlFile)
                {
                    if(processDate.Equals(startDate))
                        isFirstDate = true;
                    else
                        isFirstDate = false;

                    if(processDate.Equals(endDate))
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


                if(adventOutputType.Equals(AdventOutputType.Constituent))
                {
                    GenerateConstituentReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    sharedData.GenerateAxmlFileConstituents(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"),
                                                            sIndexName, Vendors.RussellIcb, indexRowsTickerSort,
                                                            isFirstDate, isLastDate);
                }
                else if(adventOutputType.Equals(AdventOutputType.Sector))
                {
                    GenerateIndustryReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    sharedData.GenerateAxmlFileSectors(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"),
                                                       sIndexName, Vendors.RussellIcb,
                                                       indexRowsSectorLevel1RollUp, indexRowsSectorLevel2RollUp, indexRowsSectorLevel3RollUp, indexRowsSectorLevel4RollUp,
                                                       isFirstDate, isLastDate);
                }
            }
        }


        public void GenerateConstituentReturnsForDate(string sDate, string sIndexName)
        {
            int i = 0;
            InitializeIndexRows();
            if(GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Constituent) == true)
            {
                for(bool GotNext = true; GotNext;)
                {
                    i += 1;
                    string sCusip = null;
                    string sTicker = null;
                    string sIndustry = null;
                    string sSuperSector = null;
                    string sSector = null;
                    string sSubSector = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out sIndustry, out sSuperSector, out sSector, out sSubSector,
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker,
                                                         sIndustry, sSuperSector, sSector, sSubSector,
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);
                        //indexRow.CurrentTicker = sharedData.GetSecurityMasterCurrentTickerRussell(sTicker, sCusip, sDate);
                        indexRow.CurrentTicker = sTicker;
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
                 /*
                    •	ICBIndustry, ICBSuperSector, ICBSector, ICBSubSector
                    •	Example: 50, 5010, 501010, 50101025
                */

                string SqlSelect = @"
                    SELECT h1.FileDate, h2.IndexName, h1.CUSIP, lower(h1.Ticker) as Ticker, h1.SecurityReturn, 
                    LEFT(h1.Sector,2) As IcbIndustry, LEFT(h1.Sector,4) As IcbSuperSector, LEFT(h1.Sector,6) As IcbSector, h1.Sector As IcbSubSector,
                        ROUND((( (cast(h1.MktValue as float) * 
                                    (cast(h2.SharesNumerator as float)/h1.SharesDenominator))/
                        (SELECT     
                            sum( cast(h1.MktValue as float) * 
                                 (cast(h2.SharesNumerator as float)/h1.SharesDenominator))
                         FROM         RussellIcbDailyHoldings1 h1 
                            inner join dbo.RussellIcbDailyHoldings2 h2 on 
                            h1.FileDate = h2.FileDate and 
                            h1.CUSIP = h2.CUSIP
                         WHERE 
                            h2.FileDate = @FileDate and 
                            h2.IndexName = @IndexName and 
                            h2.SharesNumerator > 0 and h1.SharesDenominator > 0)
                        ) * 100 ),12) As Weight
                    FROM         RussellIcbDailyHoldings1 h1 inner join
                          dbo.RussellIcbDailyHoldings2 h2 on 
                          h1.FileDate = h2.FileDate and 
                          h1.CUSIP = h2.CUSIP
                    WHERE 
                        h2.FileDate = @FileDate and 
                        h2.IndexName = @IndexName and 
                        h2.SharesNumerator > 0 and 
                        h1.SharesDenominator > 0
                ";

                string SqlOrderBy = "";
                switch(OutputType)
                {
                    case AdventOutputType.Constituent:
                        SqlOrderBy = @"
                        ORDER BY Ticker
                        ";
                        break;
                    case AdventOutputType.Sector:
                        SqlOrderBy = @"
                        ORDER BY Sector
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
                if(mSqlDr.HasRows)
                {
                    ReturnsGenerated = true;
                    mPrevId = "";
                    ConstituentCount = 0;
                }
            }

            catch(SqlException ex)
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
                                             /*out string sSector, out string sSubSector, out string sIndustry,*/
                                             out string sIndustry, out string sSuperSector, out string sSector, out string sSubSector,
                                             out string sWeight, out string sSecurityReturn)
        {
            string sMsg = null;
            bool GetNext = false;
            sCusip = "";
            sTicker = "";
            sIndustry = "";
            sSuperSector = "";
            sSector = "";
            sSubSector = "";
            sWeight = "";
            sSecurityReturn = "";

            try
            {
                if(mSqlDr.HasRows)
                {
                    if((GetNext = mSqlDr.Read()) == true)
                    {
                        sCusip = mSqlDr["CUSIP"].ToString();
                        string sOriginalTicker = mSqlDr["Ticker"].ToString();
                        sOriginalTicker = sOriginalTicker.ToLower();
                        sWeight = mSqlDr["Weight"].ToString();
                        sSecurityReturn = mSqlDr["SecurityReturn"].ToString();
                        sIndustry = mSqlDr["IcbIndustry"].ToString(); 
                        sSuperSector = mSqlDr["IcbSuperSector"].ToString();
                        sSector = mSqlDr["IcbSector"].ToString();
                        sSubSector = mSqlDr["IcbSubSector"].ToString();

                        string sFileDate = mSqlDr["FileDate"].ToString(); ;

                        ConstituentCount += 1;

                        string sNewCusip = GetNewCUSIP(sCusip);
                        string smCusip = null;
                        if(sCusip.Equals(sNewCusip))
                            smCusip = sCusip;
                        else
                            smCusip = sNewCusip;

                        string smTicker = GetSecurityMasterTicker(smCusip, sFileDate);
                        if(smTicker.Length > 0)
                            sTicker = smTicker;
                        else
                            sTicker = sOriginalTicker;

                        sMsg = sTicker + "," + sWeight + "," + sSecurityReturn + "," + sCusip;
                        //LogHelper.WriteLine(sMsg);


                        if((mPrevId.Length > 0) && sCusip.Equals(mPrevId))
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
                        if(ConstituentCount > 0)
                        {
                            CloseGlobals();
                        }
                    }
                }
            }

            catch(SqlException ex)
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
                         FROM         RussellIcbDailyHoldings1 h1 
                            inner join dbo.RussellIcbDailyHoldings2 h2 on 
                            h1.FileDate = h2.FileDate and 
                            h1.CUSIP = h2.CUSIP
                         WHERE 
                            h2.FileDate = @FileDate and 
                            h2.IndexName = @IndexName and 
                            h2.SharesNumerator > 0 and h1.SharesDenominator > 0)
                        ) * cast(h1.SecurityReturn as float) ),9) As WeightedCalcReturn9 
                    FROM         RussellIcbDailyHoldings1 h1 inner join
                          dbo.RussellIcbDailyHoldings2 h2 on 
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
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        string sReturn = dr[0].ToString();
                        if(sReturn == null || sReturn.Length.Equals(0))
                        {
                            sReturn = "0";
                        }
                        else
                        {
                            dReturn = (double)dr[0];
                            dReturn = Math.Round(dReturn, 9, MidpointRounding.AwayFromZero);
                            sReturn = dReturn.ToString();
                        }

                        //LogHelper.WriteLine(sDate + " " + sReturn);
                        if(bSaveReturnInDb)
                        {
                            foreach(string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                            {
                                sharedData.AddTotalReturn(oDate, sIndexName, Vendors.RussellIcb.ToString(), vendorFormat, dReturn, "AdvReturnDb");
                            }
                        }
                    }
                }
            }

            catch(SqlException ex)
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
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        sNewCUSIP = dr["newSymbol"].ToString();
                    }
                }
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        sTicker = dr["Ticker"].ToString();
                        sTicker = sTicker.ToLower();
                    }
                }

            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                SqlDelete = "delete FROM RussellIcbDailyIndexReturns ";
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
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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
                    and Vendor = 'RussellICB' and VendorFormat = 'CONSTITUENT'
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, cnSql);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@ReturnDate"].Value = oDate;

                dr = cmd.ExecuteReader();
                if(dr.HasRows)
                {
                    if(dr.Read())
                    {
                        string s = dr["VendorReturn"].ToString();
                        if(s.Length > 0 && double.TryParse(s, out double dNum))
                            dReturn = Convert.ToDouble(dr["VendorReturn"].ToString());
                    }
                }
            }
            catch(SqlException ex)
            {
                if(ex.Number == 2627)
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

            for(DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
            {
                prevDate = DateHelper.PrevBusinessDay(date);

                try
                {
                    if(mSqlConn == null)
                    {
                        mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                        mSqlConn.Open();
                    }
                    string sTotalReturn = String.Empty;
                    string sTotalReturnPrev = String.Empty;
                    string SqlSelect = @"
                    select count(*) from RussellIcbDailyIndexReturns
                    where IndexName = @IndexName 
                    and FileDate = @FileDate 
                    ";
                    SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                    cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
                    cmd.Parameters["@IndexName"].Value = sIndexName;
                    cmd.Parameters["@FileDate"].Value = prevDate;

                    int iCountPrev = (int)cmd.ExecuteScalar();
                    if(iCountPrev == 1)
                    {
                        cmd.Parameters["@FileDate"].Value = date;
                        int iCount = (int)cmd.ExecuteScalar();
                        if(iCount == 1)
                        {
                            SqlSelect = @"
                            select TotalReturn from RussellIcbDailyIndexReturns
                            where IndexName = @IndexName 
                            and FileDate = @FileDate 
                            ";
                            cmd.CommandText = SqlSelect;
                            SqlDataReader dr = cmd.ExecuteReader();
                            if(dr.HasRows)
                            {
                                if(dr.Read())
                                {
                                    sTotalReturn = dr["TotalReturn"].ToString();
                                    sTotalReturn = sTotalReturn.Trim();
                                }
                            }
                            dr.Close();
                            cmd.Parameters["@FileDate"].Value = prevDate;
                            dr = cmd.ExecuteReader();
                            if(dr.HasRows)
                            {
                                if(dr.Read())
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

                            foreach(string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                            {
                                sharedData.AddTotalReturn(date, sIndexName, Vendors.RussellIcb.ToString(), vendorFormat, CalculatedTotalReturn, "VendorReturn");
                            }
                        }
                    }
                }
                catch(SqlException ex)
                {
                    if(ex.Number == 2627)
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

            for(DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
            {
                SqlDataReader dr = null;
                try
                {
                    if(mSqlConn == null)
                    {
                        mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                        mSqlConn.Open();
                    }
                    double dVendorReturn = double.MinValue;
                    double dAdvReturn = double.MinValue;
                    double dDiff = double.MinValue;
                    int iTotalReturnPrecision = 9;


                    string SqlSelect = @"
                    select VendorReturn, AdvReturnDb from TotalReturns
                    where IndexName = @IndexName and ReturnDate = @ReturnDate 
                    and Vendor = 'RussellICB' and VendorFormat = 'CONSTITUENT'
                    ";

                    SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                    cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                    cmd.Parameters["@IndexName"].Value = sIndexName;
                    cmd.Parameters["@ReturnDate"].Value = date;

                    dr = cmd.ExecuteReader();
                    if(dr.HasRows)
                    {
                        if(dr.Read())
                        {
                            double dNum;
                            string s = "";
                            s = dr["AdvReturnDb"].ToString();
                            if(s.Length > 0 && double.TryParse(s, out dNum))
                                dAdvReturn = Convert.ToDouble(dr["AdvReturnDb"].ToString());
                            s = dr["VendorReturn"].ToString();
                            if(s.Length > 0 && double.TryParse(s, out dNum))
                                dVendorReturn = Convert.ToDouble(dr["VendorReturn"].ToString());
                            dr.Close();

                            if((dVendorReturn.Equals(double.MinValue) == false) && (dAdvReturn.Equals(double.MinValue) == false))
                            {
                                dDiff = Math.Round((dVendorReturn - dAdvReturn), iTotalReturnPrecision, MidpointRounding.AwayFromZero);

                                foreach(string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                                    sharedData.AddTotalReturn(date, sIndexName, Vendors.RussellIcb.ToString(), vendorFormat, dDiff, "DiffDb");

                                //double dTest = Math.Round((dAdvReturn + dDiff), iTotalReturnPrecision, MidpointRounding.AwayFromZero);
                            }
                        }
                    }
                }

                catch(SqlException ex)
                {
                    if(ex.Number == 2627)
                    {
                        LogHelper.WriteLine(ex.Message);
                    }
                }
                finally
                {
                    dr.Close();
                }
            }
        }

        /***************************************************/
#endregion Testing Code used by Form2.Russell Tab


        public List<string> GetClients()
        {
            return (sharedData.GetClients());
        }

        public List<string> GetClientIndices(string ClientID)
        {
            return (sharedData.GetClientIndices(ClientID));
        }

        public List<string> GetOutputTypes(string ClientID, string sIndexName)
        {
            return (sharedData.GetOutputTypes(ClientID, sIndexName));
        }


        public void CopyFilesToFtpFolder(string sFileDate, string sVendor, string dataSet, string sIndexName, string sOutputType)
        {
            Vendors vendor;

            if(sVendor.Equals("Russell"))
                vendor = Vendors.Russell;
            else if(sVendor.Equals("RussellIcb"))
                vendor = Vendors.RussellIcb;
            else if(sVendor.Equals("Snp"))
                vendor = Vendors.Snp;
            else
                vendor = Vendors.Snp;

            AdventOutputType outputType;

            if(sOutputType.Equals("Constituent"))
                outputType = AdventOutputType.Constituent;
            else if(sOutputType.Equals("Sector"))
                outputType = AdventOutputType.Sector;
            else
                outputType = AdventOutputType.Sector;

            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, dataSet, sIndexName, outputType);

            return;
        }

        public void CopyFileToFtpFolder(string clientId, string sFileDate, string sVendor, string sIndexName, string sOutputType)
        {

            if(clientId.Length > 0 && sFileDate.Length > 0 && sVendor.Length > 0 && sIndexName.Length > 0 && sOutputType.Length > 0)
            {
                Vendors vendor;

                if(sVendor.Equals("Russell"))
                    vendor = Vendors.Russell;
                else if(sVendor.Equals("RussellIcb"))
                    vendor = Vendors.RussellIcb;
                else if(sVendor.Equals("Snp"))
                    vendor = Vendors.Snp;
                else
                    vendor = Vendors.Snp;

                AdventOutputType outputType;

                if(sOutputType.Equals("Constituent"))
                    outputType = AdventOutputType.Constituent;
                else if(sOutputType.Equals("Sector"))
                    outputType = AdventOutputType.Sector;
                else
                    outputType = AdventOutputType.Sector;

                sharedData.CopyFileToFtpFolder(clientId, sFileDate, vendor, sIndexName, outputType);
            }
            else
                LogHelper.WriteLine("Make sure ClientID, FileDate, Vendor, IndexName, and Output Type are all selected");
        }
    }
}
