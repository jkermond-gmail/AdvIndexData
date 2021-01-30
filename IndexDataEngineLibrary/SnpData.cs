using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

using AdventUtilityLibrary;
using LumenWorks.Framework.IO.Csv;


namespace IndexDataEngineLibrary
{
    public sealed class SnpData
    {
        private SharedData sharedData = null;
        private SqlConnection mSqlConn = null;
        private SqlDataReader mSqlDr = null;
        private CultureInfo mEnUS = null;
        private string mPrevId;
        private int ConstituentCount = 0;
        private const string NumberFormat = "0.#########";
        private CultureInfo mCultureInfo = new CultureInfo("en-US");
        private bool mUseSnpSecurityMaster = true;

        private List<IndexRow> indexRowsTickerSort = new List<IndexRow>();
        private List<IndexRow> indexRowsIndustrySort = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel1RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel2RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel3RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel4RollUp = new List<IndexRow>();

        public SnpData()
        {
            //LogHelper.Info("SnpData()", "SnpData");
            sharedData = new SharedData();
            DateHelper.ConnectionString = sharedData.ConnectionStringAmdVifs;
            sharedData.Vendor = Vendors.Snp;
            mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
            try
            {
                mSqlConn.Open();
            }
            catch( SqlException ex)
            {
                LogHelper.WriteLine(sharedData.ConnectionStringIndexData);
                LogHelper.WriteLine(ex.Message);
            }
            finally
            {
            }

            CultureInfo mEnUS = new CultureInfo("en-US");                    
            mUseSnpSecurityMaster = AppSettings.Get<bool>("useSnpSecurityMaster");
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
            mPrevId = "";
        }

        public void TestEndOfMonthDates(string sStartDate, string sEndDate)
        {
            DateTime startDate = DateTime.Parse(sStartDate);
            DateTime endDate = DateTime.Parse(sEndDate);

            for (DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
            {
                DateHelper.IsPrevEndofMonthOnWeekend(date);
            }
        }


        #region ProcessVendorDatasetJobs

        public void ProcessVendorDatasetJobs(string Dataset, string sProcessDate)
        {
            DateTime ProcessDate = Convert.ToDateTime(sProcessDate);

            ProcessVendorFiles(ProcessDate, ProcessDate, Dataset, true, true, true, true, true);
            string IndexName = Dataset;
            if (ProcessStatus.GenerateReturns(sProcessDate, Vendors.Snp.ToString(), Dataset, IndexName))
            {
                GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Constituent, false);
                ProcessStatus.Update(sProcessDate, Vendors.Snp.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlConstituentData, ProcessStatus.StatusValue.Pass);
                GenerateReturnsForDateRange(sProcessDate, sProcessDate, IndexName, AdventOutputType.Sector, false);
                ProcessStatus.Update(sProcessDate, Vendors.Snp.ToString(), Dataset, IndexName, ProcessStatus.WhichStatus.AxmlSectorData, ProcessStatus.StatusValue.Pass);
                // Note: after this is released to production, review the work that would replace StandardAndPoors with Snp to make all tables consistent
                sharedData.VendorDatasetJobsUpdateProcessDate("StandardAndPoors", Dataset, sProcessDate);
                sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.Snp, Dataset, IndexName, AdventOutputType.Constituent);
                sharedData.CopyFilesToFtpFolder(sProcessDate, Vendors.Snp, Dataset, IndexName, AdventOutputType.Sector);
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: VendorDatasetJobs complete  " + Vendors.Snp.ToString() + " " + Dataset + " " + sProcessDate);


            }
        }

        #endregion


        #region CsvReader

        private DataTable ReadCsvIntoTable(string Filename)
        {
            DataTable dt = new DataTable();
            try
            {
                bool HasHeaders = true;
                using (CsvReader csv = 
                    new CsvReader(new StreamReader(Filename), HasHeaders, '\t', '"', '"','~', ValueTrimmingOptions.UnquotedOnly))
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

        private string ParseColumn(DataRow dr, string column)
        {
            string value = "";
            if (dr.Table.Columns.Contains(column))
            {
                if (!dr.IsNull(column))
                    value = dr[column].ToString();
            }
            return (value);
        }

        #endregion


        public string[] GetIndices()
        {
            sharedData.Vendor = Vendors.Snp;
            return (sharedData.GetIndices());
        }

        public void ProcessVendorFiles(DateTime oStartDate, DateTime oEndDate, string Dataset, bool bOpenFiles, bool bCloseFiles,
                                       bool bTotalReturnFiles, bool bSecurityMaster, bool bSymbolChanges)
        {
            DateTime oProcessDate;
            int DateCompare;
            string FilePath = AppSettings.Get<string>("VifsPath.Snp");
            string FileName = "";
            string sMsg = "ProcessVendorFiles: " + oStartDate.ToShortDateString() + " to " + oEndDate.ToShortDateString() + " " + Dataset;
            ProcessStatus.StatusValue statusValue = ProcessStatus.StatusValue.Unassigned;

            LogHelper.WriteLine(sMsg + "Started " + DateTime.Now);

            /*
             * old
_SP100_ADJ.SPC
_SP400_ADJ.SPC
_SP500_ADJ.SPC
_SP600_ADJ.SPC
_SPMLP_ADJ.SPC
_SP100_CLS.SPC
_SP400_CLS.SPC
_SP500_CLS.SPC
_SP600_CLS.SPC
_SPMLP_CLS.SPC
_SP400.SPL
_SP500.SPL
_SP600.SPL
_SP1500.SPL
_SPMLP.SPL

            * new

_SP100_ADJ.SDC
_SP400_ADJ.SDC
_SP500_ADJ.SDC
_SP600_ADJ.SDC
_SPMLP_ADJ.SDC
_SP100_CLS.SDC
_SP400_CLS.SDC
_SP500_CLS.SDC
_SP600_CLS.SDC
_SPMLP_CLS.SDC
_SP400.SDL
_SP500.SDL
_SP600.SDL
_SP900.SDL
_SP1000.SDL
_SP1500.SDL
_SPMLP.SDL
            */
            try
            {
                for (oProcessDate = oStartDate
                   ; (DateCompare = oProcessDate.CompareTo(oEndDate)) <= 0
                   ; oProcessDate = oProcessDate.AddDays(1))
                {
                    if (bOpenFiles || bSymbolChanges)
                    {
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_ADJ.SPC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }

                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_ADJ.SPC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }

                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_ADJ.SPC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }

                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_ADJ.SPC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }

                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_ADJ.SPC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                        {
                            AddSnpOpeningData(FileName, oProcessDate);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                            ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                        }

                        if (Dataset.Equals("sp900") || Dataset.Equals("sp1000") || Dataset.Equals("sp1500"))
                        {
                            statusValue = ProcessStatus.CheckStatus(oProcessDate.ToString("MM/dd/yyyy"), Vendors.Snp.ToString(), Dataset, Dataset, ProcessStatus.WhichStatus.OpenData);
                            if (statusValue.Equals(ProcessStatus.StatusValue.AssignToPass))
                            {
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.OpenData, ProcessStatus.StatusValue.Pass);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.SecurityMasterData, ProcessStatus.StatusValue.Pass);
                            }
                        }

                        if (bCloseFiles)
                        {
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_CLS.SDC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_CLS.SPC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_CLS.SDC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_CLS.SPC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_CLS.SDC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_CLS.SPC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_CLS.SDC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_CLS.SPC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_CLS.SDC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_CLS.SPC";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            {
                                AddSnpClosingData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                            if (Dataset.Equals("sp900") || Dataset.Equals("sp1000") || Dataset.Equals("sp1500"))
                            {
                                statusValue = ProcessStatus.CheckStatus(oProcessDate.ToString("MM/dd/yyyy"), Vendors.Snp.ToString(), Dataset, Dataset, ProcessStatus.WhichStatus.CloseData);
                                if (statusValue.Equals(ProcessStatus.StatusValue.AssignToPass))
                                    ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.CloseData, ProcessStatus.StatusValue.Pass);
                            }

                        }

                        if (bTotalReturnFiles)
                        {
                            // Check on this one:
                            //FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100.SDL";
                            //if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            //{
                            //    AddSnpTotalReturnData(FileName, oProcessDate);
                            //    ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            //}
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400.SPL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), "sp100", "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500.SPL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), "sp100", "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600.SPL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP900.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp900")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1000.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1000")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1500.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1500")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1500.SPL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1500")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }

                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP.SDL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                            FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP.SPL";
                            if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            {
                                AddSnpTotalReturnData(FileName, oProcessDate);
                                ProcessStatus.Update(oProcessDate, Vendors.Snp.ToString(), Dataset, "", ProcessStatus.WhichStatus.TotalReturnData, ProcessStatus.StatusValue.Pass);
                            }
                        }
                    }
                }
            }
            catch
            {
                LogHelper.WriteLine(sMsg + "Error Processing file " + FileName.ToString() + DateTime.Now);
            }
            finally
            {
                LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
            }
        }

        private string GetIndexCodeFromFilename(string Filename)
        {
            string IndexCode = "";
            // Handle the opening file eg: 20180102_SP500_ADJ.SDC to extract 500
            if (Filename.Contains("SP"))
            {
                int IndexBegin = 0;
                int IndexEnd = 0;

                IndexBegin = Filename.IndexOf("SP");

                if (Filename.Contains("_ADJ"))
                {
                    IndexEnd = Filename.IndexOf("_ADJ");
                    IndexCode = Filename.Substring(IndexBegin, IndexEnd - IndexBegin);
                }
                else if (Filename.Contains("_CLS"))
                {
                    IndexEnd = Filename.IndexOf("_CLS");
                    IndexCode = Filename.Substring(IndexBegin, IndexEnd - IndexBegin);
                }
                else if (Filename.Contains("SDL") || Filename.Contains("SPL"))
                {
                    IndexEnd = Filename.IndexOf(".");
                    IndexCode = Filename.Substring(IndexBegin, IndexEnd - IndexBegin);
                }
            }
            return (IndexCode.ToLower());
        }

        private void ConvertOldFileFormatToCsv(string Filename, out string NewFilename, out string IndexnameParsed, out string IndexCodeParsed)
        {
            NewFilename = "";
            IndexnameParsed = "";
            IndexCodeParsed = "";


            bool foundHeaderLine = false;
            StreamWriter file = null;
            string filenameTemp = null;
            for (StreamReader srFile = new StreamReader(Filename); srFile.EndOfStream == false;)
            {
                string TextLine = srFile.ReadLine();
                if (foundHeaderLine.Equals(false))
                {

                    if (TextLine.StartsWith("INDEX NAME"))
                    {
                        int index = TextLine.IndexOf("S&P");
                        IndexnameParsed = TextLine.Substring(index);
                        IndexCodeParsed = IndexnameParsed;
                    }

                    if (TextLine.StartsWith("CHANGE"))
                    {
                        foundHeaderLine = true;
                        filenameTemp = Filename + ".tmp";
                        if (File.Exists(filenameTemp))
                            File.Delete(filenameTemp);

                        file = new StreamWriter(filenameTemp);
                        file.WriteLine(TextLine);
                    }
                }
                else if (foundHeaderLine.Equals(true) && (TextLine.StartsWith("\t").Equals(true)))                    
                    file.WriteLine(TextLine);
            }

            if (file.BaseStream != null)
            {
                file.Flush();
                file.Close();
                NewFilename = filenameTemp;
            }
        }

        private void AddSnpOpeningData(string Filename, DateTime FileDate)
        {
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;
            DateTime oDate = DateTime.MinValue;
            bool isOldFormat = false;
            string IndexnameParsed = "";
            string IndexCodeParsed = "";
            //string IndexCodeParsed2 = "";

            LogHelper.WriteLine("AddSnpOpeningData Processing: " + Filename + " " + FileDate.ToShortDateString());

            string IndexCode = GetIndexCodeFromFilename(Filename);

            if (Filename.EndsWith("SPC"))
            {
                isOldFormat = true;
                string NewFilename = "";
                ConvertOldFileFormatToCsv(Filename, out NewFilename, out IndexnameParsed, out IndexCodeParsed);
                Filename = NewFilename;
                IndexCodeParsed = IndexCode;
            }

            string Tablename = " SnpDailyOpeningHoldings ";  // Note leading and trailing spaces
            SqlDelete = "delete FROM" + Tablename ;
            SqlWhere = "where FileDate = @FileDate and IndexCode = @IndexCode";
            cmd = new SqlCommand
            {
                Connection = mSqlConn,
                CommandText = SqlDelete + SqlWhere
            };

            cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
            cmd.Parameters["@FileDate"].Value = FileDate;
            cmd.Parameters.Add("@IndexCode", SqlDbType.VarChar);
            cmd.Parameters["@IndexCode"].Value = IndexCode;
            try
            {
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

            SqlWhere = SqlWhere + " and StockKey = @StockKey";

            cmd.Parameters.Add("@StockKey", SqlDbType.VarChar);
            cmd.Parameters.Add("@EffectiveDate", SqlDbType.DateTime);
            cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar);
            cmd.Parameters.Add("@Ticker", SqlDbType.VarChar);
            cmd.Parameters.Add("@GicsCode", SqlDbType.VarChar);
            cmd.Parameters.Add("@MarketCap", SqlDbType.VarChar);
            cmd.Parameters.Add("@Weight", SqlDbType.VarChar);

            DataTable dt = ReadCsvIntoTable(Filename);

            string SqlSelect = "";

            int CurrentRowCount = 0;
            int AddCount = 0;
            string sValue = "";
            int i = 0;

            foreach (DataRow dr in dt.Rows)
            {
                i += 1;
                //LogHelper.WriteLine("Processing line: " + i);

                //if( i.Equals(320))
                //    LogHelper.WriteLine("Debugiing line: " + i);


                if (isOldFormat.Equals(false))
                {
                    IndexnameParsed = ParseColumn(dr, "INDEX NAME");
                    IndexCodeParsed = ParseColumn(dr, "INDEX CODE");
                    
                    if (IndexCodeParsed.Equals("SPMLP"))
                        IndexCodeParsed = IndexCodeParsed.ToLower();
                    else
                        IndexCodeParsed = "sp" + IndexCodeParsed;
                }

                if (IndexnameParsed.StartsWith("S&P ") && IndexCodeParsed.Equals(IndexCode))
                {
                    CurrentRowCount += 1;
                    cmd.Parameters["@IndexCode"].Value = IndexCodeParsed;

                    string sStockKey = ParseColumn(dr, "STOCK KEY");    // NUMERIC but stored as String
                    cmd.Parameters["@StockKey"].Value = sStockKey;

                    string EffectiveDate = ParseColumn(dr, "EFFECTIVE DATE"); // YYYYMMDD
                    DateTime.TryParseExact(EffectiveDate, "yyyyMMdd", mEnUS, DateTimeStyles.None, out oDate);
                    cmd.Parameters["@EffectiveDate"].Value = oDate;

                    string sCUSIP = ParseColumn(dr, "CUSIP");   // 9 character
                    if (sCUSIP.Length >= 8)
                        sCUSIP = sCUSIP.Substring(0, 8);
                    else
                        LogHelper.WriteLine("Error Bad Vendor Data line: " + CurrentRowCount + " IndexCode: " + IndexCodeParsed + " Stock Key: " + sStockKey + " Short or No Cusip: " + sCUSIP);
                    cmd.Parameters["@CUSIP"].Value = sCUSIP;

                    string sTicker = ParseColumn(dr, "TICKER"); // UPPERCASE
                    cmd.Parameters["@Ticker"].Value = sTicker;

                    string sSector = ParseColumn(dr, "GICS CODE");    // NUMERIC(8)
                    cmd.Parameters["@GicsCode"].Value = sSector;

                    sValue = ParseColumn(dr, "INDEX MARKET CAP");
                    cmd.Parameters["@MarketCap"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX WEIGHT");
                    cmd.Parameters["@Weight"].Value = sValue;

                    string sCompanyName = ParseColumn(dr, "COMPANY");
                    string sExchange = "";

                    sharedData.AddSecurityMasterFull(sStockKey, sTicker, sCUSIP, "S", sCompanyName, sSector, sExchange, oDate);

                    try
                    {
                        SqlSelect = "select count(*) from" + Tablename;
                        cmd.CommandText = SqlSelect + SqlWhere;
                        int iCount = (int)cmd.ExecuteScalar();
                        if (iCount == 0)
                        {
                            cmd.CommandText =
                                "insert into " + Tablename + " (FileDate,IndexCode,StockKey,EffectiveDate,CUSIP,Ticker,GicsCode,MarketCap,Weight) " +
                                "Values (@FileDate,@IndexCode,@StockKey,@EffectiveDate,@CUSIP,@Ticker,@GicsCode,@MarketCap,@Weight)";
                            cmd.ExecuteNonQuery();
                            AddCount += 1;
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
                //else if (sValue.Equals("LINE COUNT:"))
                //{
                //    sValue = ParseColumn(dr, "INDEX CODE");
                //    int LineCount = Convert.ToInt32(sValue);
                //    LogHelper.WriteLine("finished " + DateTime.Now + " " + Filename + " adds = " + AddCount + " Linecount = " + LineCount);
                //}
            }
            LogHelper.WriteLine("AddSnpOpeningData Done: " + Filename + " " + DateTime.Now);
        }

        private void AddSnpClosingData(string Filename, DateTime FileDate)
        {
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;
            DateTime oDate = DateTime.MinValue;
            bool isOldFormat = false;
            string IndexnameParsed = "";
            string IndexCodeParsed = "";

            LogHelper.WriteLine("AddSnpClosingData Processing: " + Filename + " " + DateTime.Now);

            string IndexCode = GetIndexCodeFromFilename(Filename);

            if (Filename.EndsWith("SPC"))
            {
                isOldFormat = true;
                string NewFilename = "";
                ConvertOldFileFormatToCsv(Filename, out NewFilename, out IndexnameParsed, out IndexCodeParsed);
                Filename = NewFilename;
                IndexCodeParsed = IndexCode;
            }


            string Tablename = " SnpDailyClosingHoldings ";  // Note leading and trailing spaces
            SqlDelete = "delete FROM" + Tablename;
            SqlWhere = "where FileDate = @FileDate and IndexCode = @IndexCode";
            cmd = new SqlCommand
            {
                Connection = mSqlConn,
                CommandText = SqlDelete + SqlWhere
            };

            cmd.Parameters.Add("@FileDate", SqlDbType.DateTime);
            cmd.Parameters["@FileDate"].Value = FileDate;
            cmd.Parameters.Add("@IndexCode", SqlDbType.VarChar);
            cmd.Parameters["@IndexCode"].Value = IndexCode;
            try
            {
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

            SqlWhere = SqlWhere + " and StockKey = @StockKey";

            cmd.Parameters.Add("@StockKey", SqlDbType.VarChar);
            cmd.Parameters.Add("@EffectiveDate", SqlDbType.DateTime);
            cmd.Parameters.Add("@CUSIP", SqlDbType.VarChar);
            cmd.Parameters.Add("@Ticker", SqlDbType.VarChar);
            cmd.Parameters.Add("@GicsCode", SqlDbType.VarChar);
            cmd.Parameters.Add("@MarketCap", SqlDbType.VarChar);
            cmd.Parameters.Add("@Weight", SqlDbType.VarChar);
            cmd.Parameters.Add("@TotalReturn", SqlDbType.VarChar);

            DataTable dt = ReadCsvIntoTable(Filename);

            string SqlSelect = "";

            int CurrentRowCount = 0;
            int AddCount = 0;
            string sValue = "";

            foreach (DataRow dr in dt.Rows)
            {
                if (isOldFormat.Equals(false))
                {

                    IndexnameParsed = ParseColumn(dr, "INDEX NAME");
                    IndexCodeParsed = ParseColumn(dr, "INDEX CODE");

                    if (IndexCodeParsed.Equals("SPMLP"))
                        IndexCodeParsed = IndexCodeParsed.ToLower();
                    else
                        IndexCodeParsed = "sp" + IndexCodeParsed;

                }

                if (IndexnameParsed.StartsWith("S&P ") && IndexCodeParsed.Equals(IndexCode))
                {
                    CurrentRowCount += 1;
                    cmd.Parameters["@IndexCode"].Value = IndexCodeParsed;

                    string sStockKey = ParseColumn(dr, "STOCK KEY");    // NUMERIC but stored as String
                    cmd.Parameters["@StockKey"].Value = sStockKey;

                    string EffectiveDate = ParseColumn(dr, "EFFECTIVE DATE"); // YYYYMMDD
                    DateTime.TryParseExact(EffectiveDate, "yyyyMMdd", mEnUS, DateTimeStyles.None, out oDate);
                    cmd.Parameters["@EffectiveDate"].Value = oDate;

                    string sCUSIP = ParseColumn(dr, "CUSIP");   // 9 character
                    if (sCUSIP.Length >= 8)
                        sCUSIP = sCUSIP.Substring(0, 8);
                    else
                        LogHelper.WriteLine("Error Bad Vendor Data line: " + CurrentRowCount + " IndexCode: " + IndexCodeParsed + " Stock Key: " + sStockKey + " Short or No Cusip: " + sCUSIP);
                    cmd.Parameters["@CUSIP"].Value = sCUSIP;

                    sValue = ParseColumn(dr, "TICKER"); // UPPERCASE
                    cmd.Parameters["@Ticker"].Value = sValue;

                    sValue = ParseColumn(dr, "GICS CODE");    // NUMERIC(8)
                    cmd.Parameters["@GicsCode"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX MARKET CAP");
                    cmd.Parameters["@MarketCap"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX WEIGHT");
                    cmd.Parameters["@Weight"].Value = sValue;

                    sValue = ParseColumn(dr, "DAILY TOTAL RETURN");
                    if( isOldFormat )
                    {
                        if (sValue.Length > 0 && double.TryParse(sValue, out double dNum))
                        {
                            double dValue = Convert.ToDouble(sValue.ToString());
                            dValue = dValue * .01;
                            string NumberFormat = "0.################";
                            CultureInfo mCultureInfo = new CultureInfo("en-US");
                            sValue = dValue.ToString(NumberFormat, mCultureInfo);
                        }
                        else
                            sValue = "";
                    }

                    cmd.Parameters["@TotalReturn"].Value = sValue;

                    try
                    {
                            SqlSelect = "select count(*) from" + Tablename;
                            cmd.CommandText = SqlSelect + SqlWhere;
                            int iCount = (int)cmd.ExecuteScalar();
                            if (iCount == 0)
                            {
                                cmd.CommandText =
                                    "insert into " + Tablename + " (FileDate,IndexCode,StockKey,EffectiveDate,CUSIP,Ticker,GicsCode,MarketCap,Weight,TotalReturn) " +
                                    "Values (@FileDate,@IndexCode,@StockKey,@EffectiveDate,@CUSIP,@Ticker,@GicsCode,@MarketCap,@Weight,@TotalReturn)";
                                cmd.ExecuteNonQuery();
                                AddCount += 1;
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
            LogHelper.WriteLine("AddSnpClosingData Done: " + Filename + " " + DateTime.Now);
        }


        private void AddSnpTotalReturnData(string Filename, DateTime FileDate)
        {

            //string[] sIndices = GetVendorIndices();
            //foreach (string sIndex in sIndices)
            //{
            //    DeleteTotalReturn(FileDate, sIndex);
            //}
            LogHelper.WriteLine("AddSnpTotalReturnData Processing: " + Filename + " " + DateTime.Now);
            List<string> SearchCodeList = new List<string>();
            List<string> IndexCodeList = new List<string>();
            bool isOldFormat = false;
            string NotUsedIndexnameParsed = "";
            string NotUsedIndexCodeParsed = "";


            string IndexCode = GetIndexCodeFromFilename(Filename);

            if (Filename.EndsWith("SPL"))
            {
                isOldFormat = true;
                string NewFilename = "";
                ConvertOldFileFormatToCsv(Filename, out NewFilename, out NotUsedIndexnameParsed, out NotUsedIndexCodeParsed);
                Filename = NewFilename;
            }


            string SearchIndexCode = "";
            IndexCodeList.Add(IndexCode);
            if (IndexCode.Equals("spmlp"))
            {
                SearchIndexCode = IndexCode.ToUpper() + "T";
                SearchCodeList.Add(SearchIndexCode);
            }
            else
            {
                SearchIndexCode = IndexCode.Replace("sp", "") + "TR"; 
                SearchCodeList.Add(SearchIndexCode);
            }

            if (IndexCode.Equals("sp500"))
            {
                IndexCodeList.Add("sp100");
                SearchCodeList.Add("100TR");    // The Return for the S&P 100 is in the 500's .sdl file
            }
            else if (IndexCode.Equals("sp1500") && isOldFormat.Equals(true))
            {
                IndexCodeList.Add("sp900");
                SearchCodeList.Add("900TR");    // The Return for the S&P 900 is in the 1500's .sdl file
                IndexCodeList.Add("sp1000");
                SearchCodeList.Add("1000TR");   // The Return for the S&P 1000 is in the 1500's .sdl file
            }
            int CurrentRowCount = 0;
            string sValue = "";
            string sIndexName = "";

            DataTable dt = ReadCsvIntoTable(Filename);
            int i = 0;
            foreach (string IndexCodeSearch in SearchCodeList)
            { 
                foreach (DataRow dr in dt.Rows)
                {
                    sValue = ParseColumn(dr, "INDEX CODE");
                    sIndexName = ParseColumn(dr, "INDEX NAME");
                    //LogHelper.WriteLine("IndexCode|" + IndexCode + "| comare to |" + sValue + "|" + sIndexName + "|");
                    CurrentRowCount += 1;
                    if (sValue.Equals(IndexCodeSearch))
                    {
                        CurrentRowCount += 1;
                        sValue = ParseColumn(dr, "INDEX VALUE");
                        DeleteSnpTotalReturnForIndex(FileDate, IndexCodeList[i]);
                        AddSnpTotalReturnForIndex(FileDate, IndexCodeList[i], sValue);
                        string sStartAndEndDate = FileDate.ToString("MM/dd/yyyy");

                        sValue = ParseColumn(dr, "DAILY RETURN");
                        double TotalReturn = Convert.ToDouble(sValue);
                        TotalReturn = TotalReturn * 100;

                        int Precision = 9;
                        TotalReturn = Math.Round(TotalReturn, Precision, MidpointRounding.AwayFromZero);

                        foreach(string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                            sharedData.AddTotalReturn(sStartAndEndDate, IndexCodeList[i], Vendors.Snp.ToString(), vendorFormat, TotalReturn, "VendorReturn");


                        //CalculateVendorTotalReturnsForPeriod(sStartAndEndDate, sStartAndEndDate, IndexCodeList[i]);

                        break;
                    }
                }
                i += 1;
            }
            LogHelper.WriteLine("AddSnpTotalReturnData Done: " + Filename + " " + DateTime.Now);
        }


        public void AddSnpTotalReturnForIndex(DateTime oDate, string sIndexName, string sTotalReturn)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from SnpDailyIndexReturns
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
                        "insert into SnpDailyIndexReturns (IndexName, FileDate, TotalReturn) " +
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


        public void DeleteSnpTotalReturnForIndex(DateTime FileDate, string sIndexName)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            try
            {
                string SqlDelete;
                string SqlWhere;
                SqlCommand cmd = null;
                cnSql.Open();
                SqlDelete = "delete FROM SnpDailyIndexReturns ";
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

        //  THis is the old query which doed not roll up the weights correctly
        //         string SqlSelect = @"
        //             SELECT hclose.EffectiveDate, hopen.IndexCode, hclose.CUSIP, lower(hclose.Ticker) as Ticker, 
        //             cast(hclose.TotalReturn as float) * 100 as TotalReturn, 
        //             LEFT(hclose.GicsCode,2) As Sector, LEFT(hclose.GicsCode,4) As IndustryGroup, LEFT(hclose.GicsCode,6) As Industry, hclose.GicsCode As SubIndustry,
        //                 ROUND((( (cast(hopen.MarketCap as float) )/
        //                 (SELECT     
        //                     sum( cast(hopen.MarketCap as float))
        //                     FROM         dbo.SnpDailyClosingHoldings hclose 
        //                     inner join dbo.SnpDailyOpeningHoldings hopen on 
        //                     hclose.EffectiveDate = hopen.EffectiveDate and 
        //                     hclose.StockKey = hopen.StockKey and 
        //hclose.IndexCode = hopen.IndexCode
        //                     WHERE 
        //                     hopen.EffectiveDate = @EffectiveDate and 
        //                     (hopen.IndexCode = @IndexCode1 OR hopen.IndexCode = @IndexCode2 OR hopen.IndexCode = @IndexCode3))
        //                 ) * 100 ),12) As Weight
        //             FROM         SnpDailyClosingHoldings hclose inner join
        //                     dbo.SnpDailyOpeningHoldings hopen on 
        //                     hclose.EffectiveDate = hopen.EffectiveDate and 
        //                     hclose.StockKey = hopen.StockKey and 
        //hclose.IndexCode = hopen.IndexCode
        //             WHERE 
        //                 hopen.EffectiveDate = @EffectiveDate and 
        //                 (hopen.IndexCode = @IndexCode1 OR hopen.IndexCode = @IndexCode2 OR hopen.IndexCode = @IndexCode3)
        //         ";


        private bool GenerateReturnsForDate(string sDate, string sIndexName, AdventOutputType OutputType)
        {
            string sMsg = null;
            mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
            bool ReturnsGenerated = false;

            indexRowsTickerSort.Clear();
            indexRowsTickerSort.TrimExcess();
            indexRowsIndustrySort.Clear();
            indexRowsIndustrySort.TrimExcess();
            indexRowsSectorLevel1RollUp.Clear();
            indexRowsSectorLevel1RollUp.TrimExcess();
            indexRowsSectorLevel2RollUp.Clear();
            indexRowsSectorLevel2RollUp.TrimExcess();
            indexRowsSectorLevel3RollUp.Clear();
            indexRowsSectorLevel3RollUp.TrimExcess();
            indexRowsSectorLevel4RollUp.Clear();
            indexRowsSectorLevel4RollUp.TrimExcess();

            string sIndexCode1 = "";
            string sIndexCode2 = "";
            string sIndexCode3 = "";

            if (sIndexName.Equals("sp900"))
            {
                sIndexCode1 = "sp400";
                sIndexCode2 = "sp500";
                sIndexCode3 = "sp500";
            }
            else if (sIndexName.Equals("sp1000"))
            {
                sIndexCode1 = "sp400";
                sIndexCode2 = "sp600";
                sIndexCode3 = "sp600";
            }
            else if (sIndexName.Equals("sp1500"))
            {
                sIndexCode1 = "sp400";
                sIndexCode2 = "sp500";
                sIndexCode3 = "sp600";
            }
            else
            {
                sIndexCode1 = sIndexName;
                sIndexCode2 = sIndexName;
                sIndexCode2 = sIndexName;
            }

            try
            {
                sMsg = "GenerateReturnsForDate: " + sDate + " Index: " + sIndexName;
                LogHelper.WriteLine(sMsg + sDate);


                string SqlSelectOld = @"
                    SELECT hclose.EffectiveDate, hclose.IndexCode, hclose.CUSIP, lower(hclose.Ticker) as Ticker, 
                    cast(hclose.TotalReturn as float) * 100 as TotalReturn, 
                    LEFT(hclose.GicsCode,2) As Sector, LEFT(hclose.GicsCode,4) As IndustryGroup, LEFT(hclose.GicsCode,6) As Industry, hclose.GicsCode As SubIndustry,
                        ROUND((( (cast(hclose.MarketCap as float) )/
                        (SELECT     
                            sum( cast(hclose.MarketCap as float))
                            FROM         dbo.SnpDailyClosingHoldings hclose 
                            WHERE 
                            hclose.EffectiveDate = @EffectiveDate and 
                            (hclose.IndexCode = @IndexCode1 OR hclose.IndexCode = @IndexCode2 OR hclose.IndexCode = @IndexCode3))
                        ) * 100 ),12) As Weight
                    FROM         SnpDailyClosingHoldings hclose 
                    WHERE 
                        hclose.EffectiveDate = @EffectiveDate and 
                        (hclose.IndexCode = @IndexCode1 OR hclose.IndexCode = @IndexCode2 OR hclose.IndexCode = @IndexCode3)
                ";
                string SqlSelect = @"
                SELECT 
                  c.EffectiveDate, c.IndexCode, c.CUSIP, lower(c.Ticker) as Ticker, cast(c.TotalReturn as float) * 100 as TotalReturn,
                  LEFT(c.GicsCode,2) As Sector, LEFT(c.GicsCode,4) As IndustryGroup, LEFT(c.GicsCode,6) As Industry, c.GicsCode As SubIndustry,
                  o.[Weight]
                  FROM SnpDailyOpeningHoldings o
                  inner join SnpDailyClosingHoldings c
                  on o.StockKey = c.StockKey and o.EffectiveDate = c.EffectiveDate and o.IndexCode = c.IndexCode
                  where o.effectivedate = @EffectiveDate  and 
                  (o.IndexCode = @IndexCode1 OR o.IndexCode = @IndexCode2 OR o.IndexCode = @IndexCode3)
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
                        ORDER BY SubIndustry
                        ";
                        break;
                    default:
                        break;
                }

                //LogHelper.WriteLine(SqlSelect);
                mSqlConn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlOrderBy, mSqlConn);
                cmd.Parameters.Add("@IndexCode1", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@IndexCode2", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@IndexCode3", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@EffectiveDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexCode1"].Value = sIndexCode1;
                cmd.Parameters["@IndexCode2"].Value = sIndexCode2;
                cmd.Parameters["@IndexCode3"].Value = sIndexCode3;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@EffectiveDate"].Value = oDate;

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

        public string GetNewCUSIP(string sOldCUSIP)
        {
            string sNewCUSIP = sOldCUSIP;

            return (sNewCUSIP);
        }

        public string GetSecurityMasterTicker(string sCUSIP)
        {
            SqlConnection cnSql = new SqlConnection(sharedData.ConnectionStringIndexData);
            SqlDataReader dr = null;
            string sTicker = "";

            try
            {
                cnSql.Open();
                string SqlSelect = @"
                    SELECT Ticker FROM HistoricalSecurityMasterFull WHERE Cusip = @Cusip
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


        public bool GetNextConstituentReturn(out string sCusip, out string sTicker,
                                             out string sSector, out string sIndustryGroup, out string sIndustry, out string sSubIndustry,
                                             out string sWeight, out string sSecurityReturn)
        {
            string sMsg = null;
            bool GetNext = false;
            sCusip = "";
            sTicker = "";
            sSector = "";
            sIndustryGroup = "";
            sIndustry = "";
            sSubIndustry = "";
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
                        sSecurityReturn = mSqlDr["TotalReturn"].ToString();
                        sSector = mSqlDr["Sector"].ToString();
                        sIndustryGroup = mSqlDr["IndustryGroup"].ToString();
                        sIndustry = mSqlDr["Industry"].ToString();
                        sSubIndustry = mSqlDr["SubIndustry"].ToString();

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

//                        string smTicker = GetSecurityMasterTicker(smCusip);
                        string smTicker = "";
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
                        sMsg = "GetNextConstituentReturn:," + ConstituentCount.ToString();
                        LogHelper.WriteLine(sMsg);
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
                if (isHistoricalAxmlFile)
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
                    sharedData.GenerateAxmlFileConstituents(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"), sIndexName, Vendors.Snp, 
                                                            indexRowsTickerSort, isFirstDate, isLastDate);
                }
                else if (adventOutputType.Equals(AdventOutputType.Sector))
                { 
                    GenerateIndustryReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    sharedData.GenerateAxmlFileSectors(processDate.ToString("MM/dd/yyyy"), fileDate.ToString("MM/dd/yyyy"), sIndexName, Vendors.Snp, 
                                                       indexRowsSectorLevel1RollUp, indexRowsSectorLevel2RollUp, indexRowsSectorLevel3RollUp, indexRowsSectorLevel4RollUp,
                                                       isFirstDate, isLastDate);
                }
            }
        }


        public void GenerateConstituentReturnsForDate(string sDate, string sIndexName)
        {
            InitializeIndexRows();

            if (GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Constituent) == true)
            {
                indexRowsTickerSort.Clear();
                int i = 0;

                for (bool GotNext = true; GotNext;)
                {
                    string sCusip = null;
                    string sTicker = null;
                    string sSector = null;
                    string sIndustryGroup = null;
                    string sIndustry = null;
                    string sSubIndustry = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if ((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out sSector, out sIndustryGroup, out sIndustry, out sSubIndustry,
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker,
                                                         sSector, sIndustryGroup, sIndustry, sSubIndustry,
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);

                        if( mUseSnpSecurityMaster.Equals(true))
                            indexRow.CurrentTicker = sharedData.GetSecurityMasterCurrentTickerSnp( sTicker, sCusip, sDate);
                        else
                            indexRow.CurrentTicker = sharedData.GetSecurityMasterCurrentTickerRussell(sTicker, sCusip, sDate);
                        indexRowsTickerSort.Add(indexRow);
                        i = i + 1;
                    }
                }
                CalculateAdventTotalReturnForDate(indexRowsTickerSort, sDate, sIndexName, IndexRow.VendorFormat.CONSTITUENT.ToString());
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

            if (GenerateReturnsForDate(sDate, sIndexName, AdventOutputType.Sector) == true)
                {
                    for (bool GotNext = true; GotNext;)
                {
                    string sCusip = null;
                    string sTicker = null;
                    string SectorLevel1 = null;
                    string SectorLevel2 = null;
                    string SectorLevel3 = null;
                    string SectorLevel4 = null;
                    string sWeight = null;
                    string sSecurityReturn = null;

                    if ((GotNext =
                        GetNextConstituentReturn(out sCusip, out sTicker,
                                                 out SectorLevel1, out SectorLevel2, out SectorLevel3, out SectorLevel4,
                                                 out sWeight, out sSecurityReturn)) == true)
                    {
                        IndexRow indexRow = new IndexRow(sDate, sIndexName, sCusip, sTicker,
                                                         SectorLevel1, SectorLevel2, SectorLevel3, SectorLevel4,
                                                         sWeight, sSecurityReturn, IndexRow.VendorFormat.CONSTITUENT);
                        indexRowsIndustrySort.Add(indexRow);
                    }
                }

                for (IndexRow.VendorFormat vendorFormat = IndexRow.VendorFormat.SECTOR_LEVEL1
                    ; vendorFormat <= IndexRow.VendorFormat.SECTOR_LEVEL4
                    ; vendorFormat++)
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
                                case IndexRow.VendorFormat.SECTOR_LEVEL4:
                                    IndexRow rollUpIndexRow4 = new IndexRow(sDate, sIndexName, "", "",
                                                                     "", "", "", sCurrentIdentifier,
                                                                     rolledUpWeight.ToString(), "", vendorFormat);
                                    indexRowsSectorLevel4RollUp.Add(rollUpIndexRow4); break;

                            }
                        }
                    }

                    switch (vendorFormat)
                    {
                        case IndexRow.VendorFormat.SECTOR_LEVEL1:
                            RollUpRatesOfReturn(indexRowsSectorLevel1RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel1RollUp, sDate, sIndexName, IndexRow.VendorFormat.SECTOR_LEVEL1.ToString());
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL2:
                            RollUpRatesOfReturn(indexRowsSectorLevel2RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel2RollUp, sDate, sIndexName, IndexRow.VendorFormat.SECTOR_LEVEL2.ToString());
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL3:
                            RollUpRatesOfReturn(indexRowsSectorLevel3RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel3RollUp, sDate, sIndexName, IndexRow.VendorFormat.SECTOR_LEVEL3.ToString());
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL4:
                            RollUpRatesOfReturn(indexRowsSectorLevel4RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel4RollUp, sDate, sIndexName, IndexRow.VendorFormat.SECTOR_LEVEL4.ToString());
                            break;
                    }
                }
            }
        }

        private void CalculateAdventTotalReturnForDate(List<IndexRow> indexRows, string sDate, string sIndexName, string sVendorFormat)
        {
            int totalReturnPrecision = 9;

            IndexRows.ZeroAdventTotalReturn();
            foreach (IndexRow indexRow in indexRows)
                indexRow.CalculateAdventTotalReturn();

            double AdventTotalReturn = IndexRows.AdventTotalReturn;
            AdventTotalReturn = AdventTotalReturn * 100;
            AdventTotalReturn = Math.Round(AdventTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);

            sharedData.AddTotalReturn(sDate, sIndexName, Vendors.Snp.ToString(), sVendorFormat, AdventTotalReturn, "AdvReturn");
        }


        private double CalculateAdventTotalReturnForDateNoGood(string sDate, string sIndexName, bool bSaveReturnInDb)
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
                    SELECT ROUND((( (cast(hopen.MktValue as float)/
                        (SELECT     
                            sum( cast(hopen.MktValue as float))
                         FROM         SnpDailyOpeningHoldings hopen 
                            inner join dbo.SnpDailyClosingHoldings hclose on 
                            hopen.EffectiveDate = hclose.EffectiveDate and 
                            hopen.StockKey = hclose.StockKey
                         WHERE 
                            hclose.EffectiveDate = @EffectiveDate and 
                            hclose.IndexName = @IndexName)
                        ) * cast(hclose.TotalReturn as float) ),9) As WeightedCalcReturn9 
                    FROM         SnpDailyOpeningHoldings hopen inner join
                          dbo.SnpDailyClosingHoldings hclose on 
                          hopen.EffectiveDate = hclose.EffectiveDate and 
                          hopen.CUSIP = hclose.CUSIP
                    WHERE 
                        hclose.EffectiveDate = @EffectiveDate and 
                        hclose.IndexName = @IndexName
                    ) as SumWeightedCalcReturn9 
                ";

                //LogHelper.WriteLine(SqlSelect);
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect, conn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@EffectiveDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@EffectiveDate"].Value = oDate;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        string s = dr[0].ToString();
                        if (s.Length > 0 && double.TryParse(s, out double dNum))
                            dReturn = Convert.ToDouble(dr[0].ToString());
                        dReturn = Math.Round(dReturn, 9, MidpointRounding.AwayFromZero);
                        string sReturn = dReturn.ToString();

                        //LogHelper.WriteLine(sDate + " " + sReturn);
                        if (bSaveReturnInDb)
                        {
                            foreach (string vendorFormat in Enum.GetNames(typeof(IndexRow.VendorFormat)))
                                sharedData.AddTotalReturn(oDate, sIndexName, Vendors.Snp.ToString(), vendorFormat, dReturn, "AdvReturn");
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


        public void CalculateVendorTotalReturnsForPeriod(string sStartDate, string sEndDate, string sIndexName)
        {

            DateTime startDate = DateTime.Parse(sStartDate);
            DateTime endDate = DateTime.Parse(sEndDate);
            DateTime prevDate = DateTime.MinValue;

            for (DateTime date = startDate; date <= endDate; date = DateHelper.NextBusinessDay(date))
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
                    select count(*) from SnpDailyIndexReturns
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
                            select TotalReturn from SnpDailyIndexReturns
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
                                sharedData.AddTotalReturn(date, sIndexName, Vendors.Snp.ToString(), vendorFormat, CalculatedTotalReturn, "VendorReturn");
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
                SqlSelect = "select count (distinct FileDate) from SnpDailyClosingHoldings ";
                SqlWhere = "where FileDate >= '" + sStartDate + "' ";
                SqlWhere += "and FileDate <= '" + sEndDate + "' ";
                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    IndexReturnArray = new IndexReturnStruct[count];
                    SqlSelect = "select distinct FileDate from SnpDailyClosingHoldings ";
                    cmd.CommandText = SqlSelect + SqlWhere;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        string sDate = dr["FileDate"].ToString();
                        //LogHelper.WriteLine("Processing: " + sDate);
                        // Uncomment this line to Calculate Return from RussellDailyHoldings
                        dReturn = CalculateAdventTotalReturnForDateNoGood(sDate, sIndexName, true);
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

        public void TestFileCopy()
        {
            string clientId = "385";
            string sFileDate = "07/01/2019";
            Vendors vendor = Vendors.Russell;
            string sIndexName = "r3000";
            AdventOutputType outputType = AdventOutputType.Sector;
            sharedData.CopyFileToFtpFolder(clientId, sFileDate, vendor, sIndexName, outputType);
            clientId = "385";
            sFileDate = "06/28/2019";
            vendor = Vendors.Russell;
            sIndexName = "r3000";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFileToFtpFolder(clientId, sFileDate, vendor, sIndexName, outputType);
            clientId = "385";
            sFileDate = "06/28/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFileToFtpFolder(clientId, sFileDate, vendor, sIndexName, outputType);
            clientId = "385";
            sFileDate = "07/01/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFileToFtpFolder(clientId, sFileDate, vendor, sIndexName, outputType);
        }

        public void TestFilesCopy()
        {
            string sFileDate = "07/01/2019";
            Vendors vendor = Vendors.Russell;
            string sIndexName = "r3000";
            AdventOutputType outputType = AdventOutputType.Sector;
            ProcessStatus.UseProcessStatus = true;
            string sConnectionIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "RGS", sIndexName, outputType);
            sFileDate = "06/28/2019";
            vendor = Vendors.Russell;
            sIndexName = "r3000";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "RGS", sIndexName, outputType);
            sFileDate = "06/28/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "sp500", sIndexName, outputType);
            sFileDate = "07/01/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Sector;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "sp500", sIndexName, outputType);
            sFileDate = "06/28/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Constituent;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "sp500", sIndexName, outputType);
            sFileDate = "07/01/2019";
            vendor = Vendors.Snp;
            sIndexName = "sp500";
            outputType = AdventOutputType.Constituent;
            sharedData.CopyFilesToFtpFolder(sFileDate, vendor, "sp500", sIndexName, outputType);

        }

    }
}
