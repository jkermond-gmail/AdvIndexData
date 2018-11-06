﻿using System;
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
        private string mAxmlFilename;
        //private double mRolledUpWeight;
        //private double mRolledUpReturn;
        private const string NumberFormat = "0.#########";
        private CultureInfo mCultureInfo = new CultureInfo("en-US");


        private List<IndexRow> indexRowsTickerSort = new List<IndexRow>();
        private List<IndexRow> indexRowsIndustrySort = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel1RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel2RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel3RollUp = new List<IndexRow>();
        private List<IndexRow> indexRowsSectorLevel4RollUp = new List<IndexRow>();

        private string[] sVendorFormats = new string[]
        {
            "StandardPoorsSecurity",
            "StandardPoorsSector",
            "StandardPoorsIndustryGroup",
            "StandardPoorsIndustry",
            "StandardPoorsSubIndustry"
        };


        public SnpData()
        {
            LogHelper.Info("SnpData()", "SnpData");
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
                Console.WriteLine(sharedData.ConnectionStringIndexData);
                Console.WriteLine(ex.Message);
            }
            finally
            {
            }

            CultureInfo mEnUS = new CultureInfo("en-US");
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
            //mRolledUpWeight = 0.0;
            //mRolledUpReturn = 0.0;

        }

        #region ProcessVendorDatasetJobs

        public void ProcessVendorDatasetJobs(string Dataset, string sProcessDate)
        {
            DateTime ProcessDate = Convert.ToDateTime(sProcessDate);

            ProcessVendorFiles(ProcessDate, ProcessDate, Dataset, true, true, true, true, true);
            string sIndexName = Dataset;
            GenerateReturnsForDateRange(sProcessDate, sProcessDate, sIndexName, AdventOutputType.Constituent);
            GenerateReturnsForDateRange(sProcessDate, sProcessDate, sIndexName, AdventOutputType.Sector);
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


        /*
            using (CsvReader csv =
                       new CsvReader(new StreamReader("RIA.csv"), true))
            {
                // missing fields will not throw an exception,
                // but will instead be treated as if there was a null value
                csv.DefaultParseErrorAction = ParseErrorAction.RaiseEvent;
                // Either of the 2 formats below are fine
                //csv.ParseError += new EventHandler<ParseErrorEventArgs>(csv_ParseError);
                csv.ParseError += csv_ParseError;
                int fieldCount = csv.FieldCount;

                string[] headers = csv.GetFieldHeaders();
                int i = 0;
                DataTable dt = new DataTable();
                
                dt.Load(csv);

                while (csv.ReadNextRecord())
                {
                    i++;
                    swLogFile.Write(i.ToString() + "|");
                    swLogFile.Write(csv[0].ToString() + "|");
                    swLogFile.Write(csv[1].ToString() + "|");
                    swLogFile.Write(csv[10].ToString() + "|");
                    swLogFile.Write(csv[11].ToString() + "|");
                    swLogFile.Write(csv[12].ToString() + "|");
                    swLogFile.Write(csv[13].ToString() + "|");
                    swLogFile.Write(csv[38].ToString() + "|");
                    swLogFile.Write(csv[69].ToString() + "|");
                    swLogFile.WriteLine(csv[77].ToString() + "|");
                    swLogFile.Flush();

                    //for (int i = 0; i < fieldCount; i++)
                    //    Console.Write(string.Format("{0} = {1};",
                    //                  headers[i], csv[i]));
                    //Console.WriteLine();
                }
            }

        */

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
            string FilePath = ConfigurationManager.AppSettings["VifsPath.Snp"];
            string FileName;
            string sMsg = "ProcessVendorFiles: " + oStartDate.ToShortDateString() + " to " + oEndDate.ToShortDateString() + " " + Dataset;

            LogHelper.WriteLine(sMsg + "Started " + DateTime.Now);

            try
            {
                //FileName = FilePath + oStartDate.ToString("yyyyMMdd") + "_SP400_ADJ.SDC";
                //if (File.Exists(FileName))
                //    AddSnpOpeningData(FileName, oStartDate);

                for (oProcessDate = oStartDate
                   ; (DateCompare = oProcessDate.CompareTo(oEndDate)) <= 0
                   ; oProcessDate = oProcessDate.AddDays(1))
                {
                    if (bOpenFiles || bSymbolChanges)
                    {                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            AddSnpOpeningData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            AddSnpOpeningData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            AddSnpOpeningData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            AddSnpOpeningData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1000_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1000")))
                            AddSnpOpeningData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1500_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1500")))
                            AddSnpOpeningData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_ADJ.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            AddSnpOpeningData(FileName, oProcessDate);                            
                    }

                    if (bCloseFiles)
                    {                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            AddSnpClosingData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            AddSnpClosingData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            AddSnpClosingData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            AddSnpClosingData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1000_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1000")))
                            AddSnpClosingData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1500_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1500")))
                            AddSnpClosingData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP_CLS.SDC";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            AddSnpClosingData(FileName, oProcessDate);                        
                    }
                    if (bTotalReturnFiles)
                    {
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP100.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp100")))
                            AddSnpTotalReturnData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP400.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp400")))
                            AddSnpTotalReturnData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP500.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp500")))
                            AddSnpTotalReturnData(FileName, oProcessDate);
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP600.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp600")))
                            AddSnpTotalReturnData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SP1500.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("sp1500")))
                            AddSnpTotalReturnData(FileName, oProcessDate);                        
                        FileName = FilePath + oProcessDate.ToString("yyyyMMdd") + "_SPMLP.SDL";
                        if (File.Exists(FileName) && (Dataset.Equals("All") || Dataset.Equals("spMLP")))
                            AddSnpTotalReturnData(FileName, oProcessDate);                        
                    }
                }
            }
            catch
            {
            }
            finally
            {
                LogHelper.WriteLine(sMsg + "finished " + DateTime.Now);
                //RussellData_Finish();
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
                else if (Filename.Contains("SDL"))
                {
                    IndexEnd = Filename.IndexOf(".");
                    IndexCode = Filename.Substring(IndexBegin, IndexEnd - IndexBegin);
                }
            }
            return (IndexCode.ToLower());
        }


        /*
        // Filename 20180102_SP500_ADJ.SDC
        _ADJ.SDC - the opening file
        INDEX CODE  EFFECTIVE DATE  CUSIP TICKER  STOCK KEY   GICS CODE GROWTH VALUE INDEX MARKET CAP INDEX WEIGHT

        _CLS.SDC - the closing file
        INDEX CODE	EFFECTIVE DATE	CUSIP   TICKER	STOCK KEY	GICS CODE	GROWTH	VALUE	DAILY TOTAL RETURN

        .SDL - Total returns for all indices
        CHANGE?	DATE OF INDEX	INDEX CODE	|JK to check which fields ->|	INDEX VALUE	CLOSE MARKET CAP	CLOSE DIVISOR	CLOSE COUNT	DAILY RETURN	INDEX DIVIDEND	ADJ MARKET CAP	ADJ DIVISOR	ADJ COUNT
        */
        /*INDEX CODE  EFFECTIVE DATE  CUSIP TICKER  STOCK KEY   GICS CODE GROWTH VALUE INDEX MARKET CAP INDEX WEIGHT

         CREATE TABLE [dbo].[SnpDailyOpeningHoldings](
            [FileDate] [dbo].[DateOnly_Type] NOT NULL,
            [IndexCode] [varchar](8) NOT NULL,
            [StockKey] [varchar](7) NOT NULL,
            [EffectiveDate] [dbo].[DateOnly_Type] NOT NULL,
            [CUSIP] [varchar](9) NULL,
            [Ticker] [varchar](7) NULL,
            [GicsCode] [varchar](9) NULL,
            [MarketCap] [varchar](13) NULL,
            [Weight] [varchar](20) NULL,
            CONSTRAINT [PK_SnpDailyHoldings] PRIMARY KEY CLUSTERED 
        (
            [FileDate] ASC,
            [IndexCode] ASC,
            [StockKey] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY]

        GO
        */
        private void AddSnpOpeningData(string Filename, DateTime FileDate)
        {
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;
            DateTime oDate = DateTime.MinValue;

            LogHelper.WriteLine("AddSnpOpeningData Processing: " + Filename + " " + FileDate.ToShortDateString());

            string IndexCode = GetIndexCodeFromFilename(Filename);

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
                    Console.WriteLine(ex.Message);
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

            foreach (DataRow dr in dt.Rows)
            {
                string IndexnameParsed = ParseColumn(dr, "INDEX NAME");
                string IndexCodeParsed = ParseColumn(dr, "INDEX CODE");
                string IndexCodeParsed2;

                if (IndexCodeParsed.Equals("SPMLP"))
                    IndexCodeParsed = IndexCodeParsed.ToLower();
                else
                    IndexCodeParsed = "sp" + IndexCodeParsed;

                if (IndexCode.Equals("sp1000") && (IndexCodeParsed.Equals("sp400") || IndexCodeParsed.Equals("sp600")))
                    IndexCodeParsed = "sp1000";

                if (IndexCodeParsed.Equals("sp500"))
                    IndexCodeParsed2 = "sp900";
                else if (IndexCodeParsed.Equals("sp400"))
                    IndexCodeParsed2 = "sp900";
                else
                    IndexCodeParsed2 = "";

                if (IndexnameParsed.StartsWith("S&P ") && IndexCodeParsed.Equals(IndexCode))
                {
                    CurrentRowCount += 1;
                    cmd.Parameters["@IndexCode"].Value = IndexCodeParsed;

                    sValue = ParseColumn(dr, "STOCK KEY");    // NUMERIC but stored as String
                    cmd.Parameters["@StockKey"].Value = sValue;

                    string EffectiveDate = ParseColumn(dr, "EFFECTIVE DATE"); // YYYYMMDD
                    DateTime.TryParseExact(EffectiveDate, "yyyyMMdd", mEnUS, DateTimeStyles.None, out oDate);
                    cmd.Parameters["@EffectiveDate"].Value = oDate;

                    sValue = ParseColumn(dr, "CUSIP");   // 9 character
                    cmd.Parameters["@CUSIP"].Value = sValue;

                    sValue = ParseColumn(dr, "TICKER"); // UPPERCASE
                    cmd.Parameters["@Ticker"].Value = sValue;

                    sValue = ParseColumn(dr, "GICS CODE");    // NUMERIC(8)
                    cmd.Parameters["@GicsCode"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX MARKET CAP");
                    cmd.Parameters["@MarketCap"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX WEIGHT");
                    cmd.Parameters["@Weight"].Value = sValue;

                    try
                    {
                        int iterations = 1;
                        if (IndexCodeParsed2.Length > 0)
                            iterations = 2;

                        for (int iteration = 1; iteration <= iterations; iteration++)
                        {
                            if (iteration == 2)
                            {
                                cmd.Parameters["@IndexCode"].Value = IndexCodeParsed2;
                            }
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
                else if (sValue.Equals("LINE COUNT:"))
                {
                    sValue = ParseColumn(dr, "INDEX CODE");
                    int LineCount = Convert.ToInt32(sValue);
                    LogHelper.WriteLine("finished " + DateTime.Now + " " + Filename + " adds = " + AddCount + " Linecount = " + LineCount);
                }
            }
            LogHelper.WriteLine("AddSnpOpeningData Done: " + Filename + " " + DateTime.Now);
        }

        private void AddSnpClosingData(string Filename, DateTime FileDate)
        {
            string SqlDelete;
            string SqlWhere;
            SqlCommand cmd = null;
            DateTime oDate = DateTime.MinValue;

            LogHelper.WriteLine("AddSnpOpeningData Processing: " + Filename + " " + DateTime.Now);

            string IndexCode = GetIndexCodeFromFilename(Filename);
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
                    Console.WriteLine(ex.Message);
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
                string IndexnameParsed = ParseColumn(dr, "INDEX NAME");
                string IndexCodeParsed = ParseColumn(dr, "INDEX CODE");
                string IndexCodeParsed2;

                if (IndexCodeParsed.Equals("SPMLP"))
                    IndexCodeParsed = IndexCodeParsed.ToLower();
                else
                    IndexCodeParsed = "sp" + IndexCodeParsed;

                if (IndexCode.Equals("sp1000") && (IndexCodeParsed.Equals("sp400") || IndexCodeParsed.Equals("sp600")))
                    IndexCodeParsed = "sp1000";

                if (IndexCodeParsed.Equals("sp500"))
                    IndexCodeParsed2 = "sp900";
                else if (IndexCodeParsed.Equals("sp400"))
                    IndexCodeParsed2 = "sp900";
                else
                    IndexCodeParsed2 = "";

                if (IndexnameParsed.StartsWith("S&P ") && IndexCodeParsed.Equals(IndexCode))
                {
                    CurrentRowCount += 1;
                    cmd.Parameters["@IndexCode"].Value = IndexCodeParsed;

                    sValue = ParseColumn(dr, "STOCK KEY");    // NUMERIC but stored as String
                    cmd.Parameters["@StockKey"].Value = sValue;

                    string EffectiveDate = ParseColumn(dr, "EFFECTIVE DATE"); // YYYYMMDD
                    DateTime.TryParseExact(EffectiveDate, "yyyyMMdd", mEnUS, DateTimeStyles.None, out oDate);
                    cmd.Parameters["@EffectiveDate"].Value = oDate;

                    sValue = ParseColumn(dr, "CUSIP");   // 9 character
                    cmd.Parameters["@CUSIP"].Value = sValue;

                    sValue = ParseColumn(dr, "TICKER"); // UPPERCASE
                    cmd.Parameters["@Ticker"].Value = sValue;

                    sValue = ParseColumn(dr, "GICS CODE");    // NUMERIC(8)
                    cmd.Parameters["@GicsCode"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX MARKET CAP");
                    cmd.Parameters["@MarketCap"].Value = sValue;

                    sValue = ParseColumn(dr, "INDEX WEIGHT");
                    cmd.Parameters["@Weight"].Value = sValue;

                    sValue = ParseColumn(dr, "DAILY TOTAL RETURN");
                    cmd.Parameters["@TotalReturn"].Value = sValue;

                    try
                    {
                        int iterations = 1;
                        if (IndexCodeParsed2.Length > 0)
                            iterations = 2;

                        for (int iteration = 1; iteration <= iterations; iteration++)
                        {
                            if (iteration == 2)
                            {
                                cmd.Parameters["@IndexCode"].Value = IndexCodeParsed2;
                            }

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
                else if (sValue.Equals("LINE COUNT:"))
                {
                    sValue = ParseColumn(dr, "INDEX CODE");
                    int LineCount = Convert.ToInt32(sValue);
                    LogHelper.WriteLine("finished " + DateTime.Now + " " + Filename + " adds = " + AddCount + " Linecount = " + LineCount);
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

            string IndexCode = GetIndexCodeFromFilename(Filename);
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
            else if (IndexCode.Equals("sp1500"))
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
                        CalculateVendorTotalReturnsForPeriod(sStartAndEndDate, sStartAndEndDate, IndexCodeList[i]);
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
                    Console.WriteLine(ex.Message);
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
//            string sIndexCode2 = "";
//            string sIndexCode3 = "";

            //if (sIndexName.Equals("sp900"))
            //{
            //    sIndexCode1 = "sp400";
            //    sIndexCode2 = "sp500";
            //    sIndexCode3 = "sp500";
            //}
            //else if (sIndexName.Equals("1500"))
            //{
            //    sIndexCode1 = "sp400";
            //    sIndexCode2 = "sp500";
            //    sIndexCode3 = "sp600";
            //}
            //else
            //{
                sIndexCode1 = sIndexName;
            //    sIndexCode2 = sIndexCode1;
            //    sIndexCode3 = sIndexCode1;
            //}

            try
            {
                sMsg = "GenerateReturnsForDate: " + sDate + " Index: " + sIndexName;
                LogHelper.WriteLine(sMsg + sDate);

                /*
                USE IndexData
                    DECLARE @FileDate nvarchar(10);
                DECLARE @EffectiveDate nvarchar(10);
                DECLARE @IndexCode1 nvarchar(10);
                DECLARE @IndexCode2 nvarchar(10);
                DECLARE @IndexCode3 nvarchar(10);
                SET @FileDate = '01/04/2017';
                SET @EffectiveDate = '01/04/2018'
                    SET @IndexCode1 = '500'
                    SET @IndexCode2 = '400'
                    SET @IndexCode3 = '600'
                */
                /*
                string SqlSelect = @"
                    SELECT distinct hclose.EffectiveDate, hopen.IndexCode, hclose.CUSIP, lower(hclose.Ticker) as Ticker, hopen.StockKey,
                    cast(hclose.TotalReturn as float) * 100 as TotalReturn, 
                    LEFT(hclose.GicsCode, 2) As Sector, LEFT(hclose.GicsCode, 4) As IndustryGroup, LEFT(hclose.GicsCode, 6) As Industry, hclose.GicsCode As SubIndustry,
                        ROUND((((cast(hopen.MarketCap as float)) /
                        (SELECT
                            sum(cast(hopen.MarketCap as float))
                            FROM         dbo.SnpDailyClosingHoldings hclose
                            inner join dbo.SnpDailyOpeningHoldings hopen on
                            hclose.EffectiveDate = hopen.EffectiveDate and
                            hclose.StockKey = hopen.StockKey
                            WHERE
                            hopen.EffectiveDate = @EffectiveDate and
                            (hopen.IndexCode = @IndexCode1 OR hopen.IndexCode = @IndexCode2 OR hopen.IndexCode = @IndexCode3))
                        ) *100 ),12) As Weight
                    FROM SnpDailyClosingHoldings hclose inner join
                            dbo.SnpDailyOpeningHoldings hopen on
                            hclose.EffectiveDate = hopen.EffectiveDate and
                            hclose.StockKey = hopen.StockKey and 
							hclose.IndexCode = hopen.IndexCode
                    WHERE
                        hopen.EffectiveDate = @EffectiveDate and
                        (hopen.IndexCode = @IndexCode1 OR hopen.IndexCode = @IndexCode2 OR hopen.IndexCode = @IndexCode3)
                    ";
                */


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
                    SET @EffectiveDate = '01/03/2018'
                    SET @IndexName = '500'
										

                 */

                string SqlSelect = @"
                    SELECT hclose.EffectiveDate, hopen.IndexCode, hclose.CUSIP, lower(hclose.Ticker) as Ticker, 
                    cast(hclose.TotalReturn as float) * 100 as TotalReturn, 
                    LEFT(hclose.GicsCode,2) As Sector, LEFT(hclose.GicsCode,4) As IndustryGroup, LEFT(hclose.GicsCode,6) As Industry, hclose.GicsCode As SubIndustry,
                        ROUND((( (cast(hopen.MarketCap as float) )/
                        (SELECT     
                            sum( cast(hopen.MarketCap as float))
                            FROM         dbo.SnpDailyClosingHoldings hclose 
                            inner join dbo.SnpDailyOpeningHoldings hopen on 
                            hclose.EffectiveDate = hopen.EffectiveDate and 
                            hclose.StockKey = hopen.StockKey and 
							hclose.IndexCode = hopen.IndexCode
                            WHERE 
                            hopen.EffectiveDate = @EffectiveDate and 
                            hopen.IndexCode = @IndexCode)
                        ) * 100 ),12) As Weight
                    FROM         SnpDailyClosingHoldings hclose inner join
                            dbo.SnpDailyOpeningHoldings hopen on 
                            hclose.EffectiveDate = hopen.EffectiveDate and 
                            hclose.StockKey = hopen.StockKey and 
							hclose.IndexCode = hopen.IndexCode
                    WHERE 
                        hopen.EffectiveDate = @EffectiveDate and 
                        hopen.IndexCode = @IndexCode
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
                        ORDER BY SubIndustry
                        ";
                        break;
                    default:
                        break;
                }

                //LogHelper.WriteLine(SqlSelect);
                mSqlConn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelect + SqlOrderBy, mSqlConn);
                cmd.Parameters.Add("@IndexCode", SqlDbType.VarChar, 20);
                //cmd.Parameters.Add("@IndexCode1", SqlDbType.VarChar, 20);
                //cmd.Parameters.Add("@IndexCode2", SqlDbType.VarChar, 20);
                //cmd.Parameters.Add("@IndexCode3", SqlDbType.VarChar, 20);
                cmd.Parameters.Add("@EffectiveDate", SqlDbType.DateTime);
                cmd.Parameters["@IndexCode"].Value = sIndexCode1;
                //cmd.Parameters["@IndexCode1"].Value = sIndexCode1;
                //cmd.Parameters["@IndexCode2"].Value = sIndexCode2;
                //cmd.Parameters["@IndexCode3"].Value = sIndexCode3;
                DateTime oDate = DateTime.MinValue;
                oDate = DateTime.Parse(sDate);
                cmd.Parameters["@EffectiveDate"].Value = oDate;

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

        public void GenerateAxmlFileConstituents(string sDate, string sIndexName /*, VendorFormats vendorFormat */)
        {

            /*
             <?xml version="1.0"?>
            <AdventXML version="3.0">
            <AccountProvider name="Russell" code="rl">
            <XSXList index="r3000" date="20170609" batch="1">
            <XSXPeriod from="20170608" through="20170609" indexperfiso="usd">
            <XSXDetail type="cs" iso="usd" symbol="a" weight="0.077572343" irr="-1.56300165"/>

            </XSXPeriod>
            </XSXList>
            </AccountProvider>
            </AdventXML>
             */
            // https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/file-system/how-to-write-to-a-text-file
            // Example #3: Write only some strings in an array to a file.
            // The using statement automatically flushes AND CLOSES the stream and calls 
            // IDisposable.Dispose on the stream object.
            // NOTE: do not use FileStream for text files because it writes bytes, but StreamWriter
            // encodes the output as text.

            // rl-20170714-xse-r3000.XSX

            mAxmlFilename = "ix-" + DateHelper.ConvertToYYYYMMDD(sDate) + "-xse-" + sIndexName + ".XSX";
            string sAxmlOutputPath = ConfigurationManager.AppSettings["AxmlOutputPath"];
            string filename = (sAxmlOutputPath + mAxmlFilename);

            if (File.Exists(filename))
                File.Delete(filename);

            using (StreamWriter file = new StreamWriter(filename))
            {
                file.WriteLine("<?xml version=\"1.0\"?>");
                file.WriteLine("<AdventXML version=\"3.0\">");
                file.WriteLine("<AccountProvider name=\"StandardAndPoors\" code=\"ix\">");
                file.WriteLine("<XSXList index=\"" + sIndexName + "\" date=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" batch=\"7\">");
                file.WriteLine("<XSXPeriod from=\"" + DateHelper.PrevBusinessDay(sDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" indexperfiso=\"usd\">");

                foreach (IndexRow indexRow in indexRowsTickerSort)
                {
                    //                elSuffixDetail.SetAttribute("weight", indexRow.Weight.ToString(NumberFormat, inputFormat.OutputCultureInfo));
                    //elSuffixDetail.SetAttribute("irr", indexRow.RateOfReturn.ToString(NumberFormat, inputFormat.OutputCultureInfo));

                    file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + indexRow.Ticker + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }

                file.WriteLine("</XSXPeriod>");
                file.WriteLine("</XSXList>");
                file.WriteLine("</AccountProvider>");
                file.Write("</AdventXML>");
            }
        }

        public void GenerateAxmlFileSectors(string sDate, string sIndexName /*, VendorFormats vendorFormat */)
        {

            /*
             filename: ix-20180105-xnf-sp500.XNX

            <?xml version="1.0"?>
            <AdventXML version="3.0">
            <AccountProvider name="StandardAndPoors" code="ix">
            <XNXList index="sp500" date="20180105" batch="7">
            <XNXPeriod from="20180104" through="20180105" indexperfiso="usd" class="GICSSector">
            <XNXDetail code="10" weight="6.198880486" irr="-0.017663738"/>
            .
            .
            </XNXPeriod>
            <XNXPeriod from="20180104" through="20180105" indexperfiso="usd" class="GICSIndGrp">
            <XNXDetail code="1010" weight="6.198880486" irr="-0.017663738"/>
            .
            .
            </XNXPeriod>
            <XNXPeriod from="20180104" through="20180105" indexperfiso="usd" class="GICSIndustry">
            <XNXDetail code="101010" weight="0.848158765" irr="0.392416422"/>
            .
            .
            </XNXPeriod>
            <XNXPeriod from="20180104" through="20180105" indexperfiso="usd" class="GICSSubInd">
            <XNXDetail code="10101010" weight="0.03102317" irr="0.451807229"/>
            .
            .
            </XNXPeriod>
            </XNXList>
            </AccountProvider>
            </AdventXML>

             */

            mAxmlFilename = "ix-" + DateHelper.ConvertToYYYYMMDD(sDate) + "-xnf-" + sIndexName + ".XNX";
            string sAxmlOutputPath = ConfigurationManager.AppSettings["AxmlOutputPath"];
            string filename = (sAxmlOutputPath + mAxmlFilename);

            if (File.Exists(filename))
                File.Delete(filename);

            using (StreamWriter file = new StreamWriter(filename))
            {
                file.WriteLine("<?xml version=\"1.0\"?>");
                file.WriteLine("<AdventXML version=\"3.0\">");
                file.WriteLine("<AccountProvider name=\"StandardAndPoors\" code=\"ix\">");
                file.WriteLine("<XNXList index=\"" + sIndexName + "\" date=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" batch=\"7\">");

                file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" indexperfiso=\"usd\" class=\"GICSSector\"> ");
                foreach (IndexRow indexRow in indexRowsSectorLevel1RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel1 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" indexperfiso=\"usd\" class=\"GICSIndGrp\"> ");
                foreach (IndexRow indexRow in indexRowsSectorLevel2RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel2 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" indexperfiso=\"usd\" class=\"GICSIndustry\"> ");
                foreach (IndexRow indexRow in indexRowsSectorLevel3RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel3 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sDate) + "\" indexperfiso=\"usd\" class=\"GICSSubInd\"> ");
                foreach (IndexRow indexRow in indexRowsSectorLevel4RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel4 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                file.WriteLine("</XNXList>");
                file.WriteLine("</AccountProvider>");
                file.Write("</AdventXML>");
            }
        }

        public void GenerateReturnsForDateRange(string sStartDate, string sEndDate, string sIndexName, AdventOutputType adventOutputType)
        {       
            DateTime startDate = Convert.ToDateTime(sStartDate);
            DateTime endDate = Convert.ToDateTime(sEndDate);
            DateTime processDate;
            int DateCompare;

            for (processDate = startDate
               ; (DateCompare = processDate.CompareTo(endDate)) <= 0
               ; processDate = processDate.AddDays(1))
            {
                if (adventOutputType.Equals(AdventOutputType.Constituent))
                { 
                    GenerateConstituentReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    GenerateAxmlFileConstituents(processDate.ToString("MM/dd/yyyy"), sIndexName);
                }
                else if (adventOutputType.Equals(AdventOutputType.Sector))
                { 
                    GenerateIndustryReturnsForDate(processDate.ToString("MM/dd/yyyy"), sIndexName);
                    GenerateAxmlFileSectors(processDate.ToString("MM/dd/yyyy"), sIndexName);
                }
            }
        }


        public void GenerateConstituentReturnsForDate(string sDate, string sIndexName)
        {
            indexRowsTickerSort.Clear();

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
                        indexRowsTickerSort.Add(indexRow);
                        i = i + 1;
                    }
                }
                CalculateAdventTotalReturnForDate(indexRowsTickerSort, sDate, sIndexName, sVendorFormats[(int)IndexRow.VendorFormat.CONSTITUENT]);
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


        public void GenerateIndustryReturnsForDate(string sDate, string sIndexName)
        {
            indexRowsIndustrySort.Clear();
            indexRowsSectorLevel1RollUp.Clear();
            indexRowsSectorLevel2RollUp.Clear();
            indexRowsSectorLevel3RollUp.Clear();

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
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel1RollUp, sDate, sIndexName, sVendorFormats[(int)IndexRow.VendorFormat.SECTOR_LEVEL1]);
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL2:
                            RollUpRatesOfReturn(indexRowsSectorLevel2RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel2RollUp, sDate, sIndexName, sVendorFormats[(int)IndexRow.VendorFormat.SECTOR_LEVEL2]);
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL3:
                            RollUpRatesOfReturn(indexRowsSectorLevel3RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel3RollUp, sDate, sIndexName, sVendorFormats[(int)IndexRow.VendorFormat.SECTOR_LEVEL3]);
                            break;
                        case IndexRow.VendorFormat.SECTOR_LEVEL4:
                            RollUpRatesOfReturn(indexRowsSectorLevel4RollUp, indexRowsIndustrySort, vendorFormat, sDate, sIndexName);
                            CalculateAdventTotalReturnForDate(indexRowsSectorLevel4RollUp, sDate, sIndexName, sVendorFormats[(int)IndexRow.VendorFormat.SECTOR_LEVEL4]);

                            break;

                    }
                }
            }
        }

        private void CalculateAdventTotalReturnForDate(List<IndexRow> indexRows, string sDate, string sIndexName, string sVendorFormat)
        {
            int totalReturnPrecision = 9;

            //double VendorTotalReturn = GetVendorTotalReturnForDate(sDate, sIndexName);

            //VendorTotalReturn = Math.Round(VendorTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);

            int i = 0;
            foreach (IndexRow indexRow in indexRows)
            {
                if (i == 0)
                {
                    indexRow.ZeroAdventTotalReturn();
                    i++;
                }
                indexRow.CalculateAdventTotalReturn();
            }

            double AdventTotalReturn = indexRows[0].AdventTotalReturn;
            AdventTotalReturn = Math.Round(AdventTotalReturn, totalReturnPrecision, MidpointRounding.AwayFromZero);

            sharedData.AddTotalReturn(sDate, sIndexName, sVendorFormat, AdventTotalReturn, "AdvReturn");

            //double AdventVsVendorDiff = VendorTotalReturn - AdventTotalReturn;

            //AdventVsVendorDiff = Math.Round(AdventVsVendorDiff, totalReturnPrecision, MidpointRounding.AwayFromZero);

            //indexRows[0].CalculateAddlContribution(AdventVsVendorDiff);

            //foreach (IndexRow indexRow in indexRows)
            //{
            //    indexRow.CalculateAdventAdjustedReturn();
            //}

            //double AdventTotalReturnAdjusted = indexRows[0].AdventTotalReturnAdjusted;
            //AdventTotalReturnAdjusted = Math.Round(AdventTotalReturnAdjusted, totalReturnPrecision, MidpointRounding.AwayFromZero);
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
                            foreach (string sVendorFormat in sVendorFormats)
                                sharedData.AddTotalReturn(oDate, sIndexName, sVendorFormat, dReturn, "AdvReturn");

                            //for (int i = 0; i < sVendorFormats.Length; i++)
                            //{
                            //    string sVendorFormat = sVendorFormats[i];
                            //    AddTotalReturn(oDate, sIndexName, sVendorFormat, dReturn, "AdvReturn");
                            //}
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

                            foreach (string sVendorFormat in sVendorFormats)
                                sharedData.AddTotalReturn(date, sIndexName, sVendorFormat, CalculatedTotalReturn, "VendorReturn");
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
    }
}
