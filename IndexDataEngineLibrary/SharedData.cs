using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using AdventUtilityLibrary;
using System.Configuration;
using System.IO;
using System.Globalization;
using System.Collections;

namespace IndexDataEngineLibrary
{
    public enum Vendors
    {
        Russell,
        Snp
    }

    public enum AdventOutputType
    {
        Constituent,
        Sector
    }

    public struct IndexReturnStruct
    {
        public double IndexReturn1;
        public double IndexReturn2;
        public string IndexDate;
    }


    internal sealed class SharedData
    {
        //private string mConnectionString = "server=VSTGMDDB2-1;database=IndexData;uid=sa;pwd=M@gichat!";
        //private string mConnectionString = @"server=JKERMOND-NEW\SQLEXPRESS2014;database=IndexData;uid=sa;pwd=M@gichat!";
        private string mConnectionStringAmdVifs = null;
        private string mConnectionStringIndexData = null;
        private SqlConnection mSqlConn = null;
        private const string NumberFormat = "0.#########";
        private CultureInfo mCultureInfo = new CultureInfo("en-US");

        Vendors mVendor;

        internal Vendors Vendor
        {
            get { return mVendor; }
            set { mVendor = value; }
        }

        public string ConnectionStringIndexData
        {
            get { return mConnectionStringIndexData; }
        }

        public string ConnectionStringAmdVifs
        {
            get { return mConnectionStringAmdVifs; }
        }

        public SharedData()
        {
            mConnectionStringIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            mConnectionStringAmdVifs = ConfigurationManager.ConnectionStrings["dbConnectionAmdVifs"].ConnectionString;
        }

        public void AddSecurityMasterFull(string Ticker, string Cusip, string Vendor, string CompanyName, string SectorCode, DateTime EndDate)
        {
            AddSecurityMasterFull("", Ticker, Cusip, Vendor, CompanyName, SectorCode, EndDate);
        }


        public void AddSecurityMasterFull(string StockKey, string Ticker, string Cusip, string Vendor, string CompanyName, string SectorCode, DateTime EndDate)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(ConnectionStringIndexData);
                    mSqlConn.Open();
                }
                CultureInfo enUS = new CultureInfo("en-US");

                string SqlSelect = @"
                    select count(*) from HistoricalSecurityMasterFull
                    where Ticker = @Ticker
                    and Cusip = @Cusip
                    and Vendor = @Vendor
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@Ticker", SqlDbType.VarChar);
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters["@Ticker"].Value = Ticker;
                cmd.Parameters["@Cusip"].Value = Cusip;
                cmd.Parameters["@Vendor"].Value = Vendor;
                int iCount = (int)cmd.ExecuteScalar();

                if( iCount == 0 )
                {
                    // But check if the ticker exists with a different CUSIP
                    SqlSelect = @"
                        select count(*) from HistoricalSecurityMasterFull
                        where Ticker = @Ticker
                        and Vendor = @Vendor
                    ";
                    cmd.CommandText = SqlSelect;
                    iCount = (int)cmd.ExecuteScalar();

                    // If the ticker exists with a different CUSIP
                    if (iCount == 1)
                    {
                        // see if the ticker exists with a "." extension eq tick.1 or tick.2 and how many
                        cmd.Parameters["@Ticker"].Value = Ticker + ".%";

                        SqlSelect = @"
                            select count(*) from HistoricalSecurityMasterFull
                            where Ticker like @Ticker
                            and Vendor = @Vendor
                        ";
                        cmd.CommandText = SqlSelect;
                        iCount = (int)cmd.ExecuteScalar();
                        iCount += 1;

                        // set ticker back to the original ticker value and determine the CUSIP
                        cmd.Parameters["@Ticker"].Value = Ticker;
                        SqlSelect = @"
                            select Cusip from HistoricalSecurityMasterFull
                            where Ticker = @Ticker
                            and Vendor = @Vendor
                        ";
                        cmd.CommandText = SqlSelect;
                        SqlDataReader dr = null;
                        dr = cmd.ExecuteReader();
                        if (dr.HasRows)
                        {
                            if (dr.Read())
                            {
                                cmd.Parameters["@Cusip"].Value = dr["CUSIP"].ToString();
                                dr.Close();
                                cmd.CommandText =
                                    "update HistoricalSecurityMasterFull  set " +
                                    "Ticker = '" + Ticker + "." + iCount.ToString() + "' " +
                                    "where Ticker = @Ticker and Cusip = @Cusip and Vendor = @Vendor";
                                cmd.ExecuteNonQuery();
                            }
                        }
                        dr.Close();
                    }
                    cmd.Parameters["@Cusip"].Value = Cusip;
                    cmd.Parameters.Add("@BeginDate", SqlDbType.DateTime);
                    cmd.Parameters["@BeginDate"].Value = EndDate;
                    cmd.CommandText =
                        "insert into HistoricalSecurityMasterFull (Ticker, Cusip, StockKey, Vendor, CompanyName, SectorCode, BeginDate, EndDate) " +
                        "Values ( @Ticker, @Cusip, @StockKey, @Vendor, @CompanyName, @SectorCode, @BeginDate, @EndDate ) ";
                }
                else
                {
                    cmd.CommandText =
                        "update HistoricalSecurityMasterFull  set " +
                        "StockKey = @StockKey, " +
                        "CompanyName = @CompanyName, " +
                        "SectorCode = @SectorCode, " +
                        "EndDate = @EndDate " +
                        "where Ticker = @Ticker and Cusip = @Cusip and Vendor = @Vendor";
                }
                cmd.Parameters.Add("@StockKey", SqlDbType.VarChar);
                cmd.Parameters["@StockKey"].Value = StockKey;
                cmd.Parameters.Add("@CompanyName", SqlDbType.VarChar);
                cmd.Parameters["@CompanyName"].Value = CompanyName;
                cmd.Parameters.Add("@SectorCode", SqlDbType.VarChar);
                cmd.Parameters["@SectorCode"].Value = SectorCode;
                cmd.Parameters.Add("@EndDate", SqlDbType.DateTime);
                cmd.Parameters["@EndDate"].Value = EndDate;
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

        public string GetSecurityMasterCurrentCusip(string OriginalCusip, string Vendor, string sDate)
        {
            string currentCusip = OriginalCusip;
            string sChangeDate = sDate;

            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                bool found = false;
                string SqlSelect = @"
                    select NewSymbol, ChangeDate from HistoricalSymbolChanges
                    where OldSymbol = @Cusip and
                    ChangeDate > @ChangeDate
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                SqlDataReader dr = null;
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
                cmd.Parameters.Add("@ChangeDate", SqlDbType.DateTime);

                while ( found.Equals(false))
                {
                    cmd.Parameters["@Cusip"].Value = currentCusip;
                    cmd.Parameters["@ChangeDate"].Value = sChangeDate;

                    dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        if (dr.Read())
                        {
                            currentCusip = dr["NewSymbol"].ToString();
                            sChangeDate = dr["ChangeDate"].ToString(); 
                            dr.Close();
                        }
                    }
                    else
                        found = true;
                    dr.Close();
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
                if (!currentCusip.Equals(OriginalCusip))
                {
                 //   LogHelper.WriteLine("GetSecurityMasterCurrentCusipRussell Current, " + currentCusip + ", Original," + OriginalCusip);
                }
            }

            return (currentCusip);
        }

        public string GetSecurityMasterCurrentTickerRussell(string Ticker, string Cusip, string sDate  )
        {
            string SecurityMasterCurrentTicker = Ticker;
            string Vendor = "R";

            string CurrentCusip = GetSecurityMasterCurrentCusip(Cusip, Vendor, sDate);

            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                string SqlSelect = @"
                    select Ticker from HistoricalSecurityMaster
                    where Cusip = @Cusip
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                SqlDataReader dr = null;
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
                cmd.Parameters["@Cusip"].Value = CurrentCusip;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        SecurityMasterCurrentTicker = dr["Ticker"].ToString();
                        SecurityMasterCurrentTicker = SecurityMasterCurrentTicker.ToLower();
                        dr.Close();
                    }
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
                if(!Ticker.Equals(SecurityMasterCurrentTicker))
                {
                   // LogHelper.WriteLine("GetSecurityMasterCurrentTickerRussell: " + Ticker + "," + SecurityMasterCurrentTicker + "," + Cusip + "," + CurrentCusip);
                }
            }
            return (SecurityMasterCurrentTicker);
        }

        public string GetSecurityMasterCurrentTickerSnp(string Ticker, string Cusip, string sDate)
        {
            string SecurityMasterCurrentTicker = Ticker;
            string Vendor = "S";
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                //string SqlSelect = @"
                //    select Ticker from HistoricalSecurityMasterFull
                //    where Cusip = @Cusip
                //    and Vendor = @Vendor
                //    ";
                string SqlSelect = @"
                select top 1 ticker from HistoricalSecurityMasterFull where StockKey in 
                (select StockKey from HistoricalSecurityMasterFull 
                 where Cusip = @Cusip and Vendor = @Vendor)
                order by EndDate desc
                ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                SqlDataReader dr = null;
                cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters["@Cusip"].Value = Cusip;
                cmd.Parameters["@Vendor"].Value = Vendor;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        SecurityMasterCurrentTicker = dr["Ticker"].ToString();
                        SecurityMasterCurrentTicker = SecurityMasterCurrentTicker.ToLower();
                        dr.Close();
                    }
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
                if (!Ticker.Equals(SecurityMasterCurrentTicker))
                {
                    //LogHelper.WriteLine("GetSecurityMasterCurrentTickerSnp:" + Ticker + ", " + SecurityMasterCurrentTicker + "," + Cusip);
                    //GetSecurityMasterCurrentTickerRussell(Ticker, Cusip, sDate);
                    //LogHelper.WriteLine(" ");
                }
            }
            return (SecurityMasterCurrentTicker);

        }


        //public string GetSecurityMasterCurrentTickerInDev(string Ticker, string Cusip, string Vendor, string sDate)
        //{
        //    string SecurityMasterCurrentTicker = Ticker;
        //    try
        //    {
        //        if (mSqlConn == null)
        //        {
        //            mSqlConn = new SqlConnection(ConnectionStringIndexData);
        //            mSqlConn.Open();
        //        }

        //        string SqlSelect = @"
        //            select StockKey from HistoricalSecurityMasterFull
        //            where Ticker = @Ticker
        //            and Cusip = @Cusip
        //            and Vendor = @Vendor
        //            ";
        //        SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
        //        SqlDataReader dr = null;
        //        string StockKey = "";
        //        cmd.Parameters.Add("@Ticker", SqlDbType.VarChar);
        //        cmd.Parameters.Add("@Cusip", SqlDbType.VarChar);
        //        cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
        //        cmd.Parameters["@Ticker"].Value = Ticker;
        //        cmd.Parameters["@Cusip"].Value = Cusip;
        //        cmd.Parameters["@Vendor"].Value = Vendor;

        //        dr = cmd.ExecuteReader();
        //        if (dr.HasRows)
        //        {
        //            if (dr.Read())
        //            {
        //                StockKey = dr["StockKey"].ToString();
        //                dr.Close();
        //                if (StockKey.Length > 0)
        //                {
        //                    cmd.Parameters.Add("@TickerDate", SqlDbType.DateTime);
        //                    cmd.Parameters["@TickerDate"].Value = sDate;
        //                    cmd.Parameters.Add("@StockKey", SqlDbType.VarChar);
        //                    cmd.Parameters["@StockKey"].Value = StockKey;

        //                    SqlSelect = @"
        //                    select Top 1 Ticker from HistoricalSecurityMasterFull
        //                    where StockKey = @StockKey
        //                    order by EndDate desc
        //                    ";
        //                    cmd.CommandText = SqlSelect;
        //                    dr = cmd.ExecuteReader();
        //                    if (dr.HasRows)
        //                    {
        //                        if (dr.Read())
        //                        {
        //                            SecurityMasterCurrentTicker = dr["Ticker"].ToString();
        //                        }
        //                    }
        //                    dr.Close();
        //                }
        //            }
        //        }
        //        dr.Close();
        //    }
        //    catch (SqlException ex)
        //    {
        //        if (ex.Number == 2627)
        //        {
        //            LogHelper.WriteLine(ex.Message);
        //        }
        //    }
        //    finally
        //    {

        //    }
        //    return (SecurityMasterCurrentTicker);

        //}


        public void AddSymbolChange(string sVendor, DateTime oChangeDate, string sOldSymbol, string sNewSymbol, string sCompanyName)
        {
            /*
            CREATE TABLE [dbo].[HistoricalSymbolChanges](
                [ChangeDate] [datetime] NOT NULL,
                [OldSymbol] [varchar](10) NOT NULL,
                [NewSymbol] [varchar](10) NOT NULL,
                [CompanyName] [varchar](30) NULL
            ) ON [PRIMARY]
            */
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(ConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from HistoricalSymbolChanges
                    where ChangeDate = @ChangeDate 
                    and Vendor = @Vendor
                    and OldSymbol = @OldSymbol 
                    and NewSymbol = @NewSymbol 
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@ChangeDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters.Add("@OldSymbol", SqlDbType.VarChar);
                cmd.Parameters.Add("@NewSymbol", SqlDbType.VarChar);
                cmd.Parameters["@ChangeDate"].Value = oChangeDate;
                cmd.Parameters["@Vendor"].Value = sVendor;
                cmd.Parameters["@OldSymbol"].Value = sOldSymbol;
                cmd.Parameters["@NewSymbol"].Value = sNewSymbol;
                int iCount = (int)cmd.ExecuteScalar();

                if (iCount == 0)
                {
                    cmd.CommandText =
                        "insert into HistoricalSymbolChanges (ChangeDate, Vendor, OldSymbol, NewSymbol, CompanyName) " +
                        "Values (@ChangeDate, @Vendor, @OldSymbol, @NewSymbol, @CompanyName)";
                    cmd.Parameters.Add("@CompanyName", SqlDbType.VarChar);
                    cmd.Parameters["@CompanyName"].Value = sCompanyName;
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



        public string[] GetIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(mConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                string SqlSelectCount = "select count(AdventIndexName) from VendorIndexMap ";
                string SqlSelect = "select AdventIndexName from VendorIndexMap ";
                string SqlWhere = "";
                if (mVendor.Equals(Vendors.Russell))
                    SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                else if (mVendor.Equals(Vendors.Snp))
                    SqlWhere = "where Vendor = 'StandardAndPoors' and Supported = 'Yes' ";
                string SqlOrderBy = "order by AdventIndexName";

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
                        Indices[i] = dr["AdventIndexName"].ToString();
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

        public string[] GetVendorIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(mConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                string SqlSelectCount = "select count(AdventIndexName) from VendorIndexMap ";
                string SqlSelect = "select AdventIndexName from VendorIndexMap ";
                string SqlWhere = "";
                if (mVendor.Equals(Vendors.Russell))
                    SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                else if (mVendor.Equals(Vendors.Snp))
                    SqlWhere = "where Vendor = 'StandardAndPoors' and Supported = 'Yes' ";
                string SqlOrderBy = "order by AdventIndexName";

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
                        Indices[i] = dr["AdventIndexName"].ToString();
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

        public void AddTotalReturn(string sDate, string sIndexName, string sVendor, string sVendorFormat,
                                   double dReturn, string sWhichReturn)
        {
            DateTime oDate = DateTime.Parse(sDate);
            AddTotalReturn(oDate, sIndexName, sVendor, sVendorFormat, dReturn, sWhichReturn);
        }


        public void AddTotalReturn(DateTime oDate, string sIndexName, string sVendor, string sVendorFormat,
                                   double dReturn, string sWhichReturn)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from TotalReturns
                    where IndexName = @IndexName 
                    and ReturnDate = @ReturnDate 
                    and Vendor = @Vendor
                    and VendorFormat = @VendorFormat
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters.Add("@VendorFormat", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                cmd.Parameters["@ReturnDate"].Value = oDate;
                cmd.Parameters["@Vendor"].Value = sVendor;
                cmd.Parameters["@VendorFormat"].Value = sVendorFormat;
                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText =
                        "insert into TotalReturns (IndexName, ReturnDate, Vendor, VendorFormat, " + sWhichReturn + ") " +
                        "Values (@IndexName, @ReturnDate, @Vendor, @VendorFormat, @" + sWhichReturn + ")";
                }
                else
                {
                    cmd.CommandText =
                        "update TotalReturns set " + sWhichReturn + " = @" + sWhichReturn + " " +
                        "where IndexName = @IndexName and ReturnDate = @ReturnDate and Vendor = @Vendor and VendorFormat = @VendorFormat";
                }
                cmd.Parameters.Add("@" + sWhichReturn, SqlDbType.Float, 8);
                cmd.Parameters["@" + sWhichReturn].Value = dReturn;
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

        /*
        Support two cases:

            1. last business day on a Friday and last calendar day is over the weekend
            last business day is Fri 06/28/19
            last calendar day is Sun 06/30/19
            Output should be:
            Filename should have end of month filename
            ix-20190630-xse-sp500.XSX
            Followed by returns for the business day from the prev business day
            <?xml version="1.0"?>
            <AdventXML version="3.0">
            <AccountProvider name="StandardAndPoors" code="ix">
            <XSXList index="sp500" date="20190630" batch="7">
            <XSXPeriod from="20190627" through="20190628" indexperfiso="usd">
            <XSXDetail type="cs" iso="usd" symbol="a" weight="0.095238957" irr="2.022134171"/>
            <XSXDetail type="cs" iso="usd" symbol="aal" weight="0.052414972" irr="1.430793157"/>
            ...
            </XSXPeriod>
            Followed by dummied returns for the period from the last business day to the end of month day
            <XSXPeriod from="20190628" through="20190630" indexperfiso="usd">
            <XSXDetail type="cs" iso="usd" symbol="a" weight="0.095238957" irr="0"/>
            <XSXDetail type="cs" iso="usd" symbol="aal" weight="0.052414972" irr="0"/>
            <XSXDetail type="cs" iso="usd" symbol="aap" weight="0.044817006" irr="0"/>
            </XSXPeriod>
            </XSXList>
            </AccountProvider>
            </AdventXML>

            1. business day on a Monday or Tues if Monday is a holiday and last calendar day is over the previous weekend
            Filename has the business day
            ix-20190701-xse-sp500.XSX
            Followed by dummied returns for the period from the last business day to the end of month day
            <?xml version="1.0"?>
            <AdventXML version="3.0">
            <AccountProvider name="StandardAndPoors" code="ix">
            <XSXList index="sp500" date="20190701" batch="7">
            <XSXPeriod from="20190628" through="20190630" indexperfiso="usd">
            <XSXDetail type="cs" iso="usd" symbol="a" weight="0.096560495" irr="0"/>
            <XSXDetail type="cs" iso="usd" symbol="aal" weight="0.05283426" irr="0"/>
            ...
            </XSXPeriod>
            Followed by return from the end of month day to the business day
            <XSXPeriod from="20190630" through="20190701" indexperfiso="usd">
            <XSXDetail type="cs" iso="usd" symbol="a" weight="0.096560495" irr="1.706173832"/>
            <XSXDetail type="cs" iso="usd" symbol="aal" weight="0.05283426" irr="0.827966881"/>
            </XSXPeriod>
            </XSXList>
            </AccountProvider>
            </AdventXML>
         */
        public void GenerateAxmlFileConstituents(string sBusinessDate, string sFileDate, string sIndexName, Vendors vendor, List<IndexRow> indexRowsTickerSort, bool IsFirstDate, bool IsLastDate)
        {
            string mAxmlFilename = "";
            bool addDummyEndOfMonthAxmlAfter = false;
            bool addDummyEndOfMonthAxmlBefore = DateHelper.IsPrevEndofMonthOnWeekend(sBusinessDate);

            DateTime prevEndofMonthDate = DateHelper.PrevEndOfMonthDay(sBusinessDate);
            string sPrevEndOfMonthDate = prevEndofMonthDate.ToString("yyyyMMdd");
            string sPrevBusinessDate = DateHelper.PrevBusinessDay(sBusinessDate);
            string sEndOfMonthDate = "";


            SortedList sortedList = new SortedList();

            // fuk  need to deal with duplicates
            foreach (IndexRow indexRow in indexRowsTickerSort)
            {
                double rateOfReturn = 0.0;
                if (vendor.Equals(Vendors.Russell))
                    rateOfReturn = indexRow.RateOfReturnAdjusted;
                else if(vendor.Equals(Vendors.Snp))
                    rateOfReturn = indexRow.RateOfReturn;

                if ( sortedList.Contains(indexRow.CurrentTicker) == false )
                    sortedList.Add(indexRow.CurrentTicker, indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "|" + rateOfReturn.ToString(NumberFormat, mCultureInfo));
                else
                    sortedList.Add(indexRow.CurrentTicker + ".dup", indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "|" + rateOfReturn.ToString(NumberFormat, mCultureInfo));
            }

            // https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/file-system/how-to-write-to-a-text-file
            // Example #3: Write only some strings in an array to a file.
            // The using statement automatically flushes AND CLOSES the stream and calls 
            // IDisposable.Dispose on the stream object.
            // NOTE: do not use FileStream for text files because it writes bytes, but StreamWriter
            // encodes the output as text.

            // rl-20170714-xse-r3000.XSX

            string prefix = "";
            string accountProvider = "";
            string batch = "";
            if (vendor.Equals(Vendors.Snp))
            {
                prefix = "ix-";
                accountProvider = "<AccountProvider name=\"StandardAndPoors\" code=\"ix\">";
                batch = "batch=\"7\">";
            }
            else if (vendor.Equals(Vendors.Russell))
            {
                prefix = "rl-";
                accountProvider = "<AccountProvider name=\"Russell\" code=\"rl\">";
                batch = "batch=\"1\">";
            }

            if (IsFirstDate && IsLastDate)
            {
                addDummyEndOfMonthAxmlAfter = DateHelper.IsEndofMonthOnWeekend(sBusinessDate);

                if( addDummyEndOfMonthAxmlAfter)
                {
                    DateTime dt = DateHelper.EndOfMonthDay(sFileDate);
                    sFileDate = dt.ToString("MM/dd/yyyy");
                    sEndOfMonthDate = DateHelper.ConvertToYYYYMMDD(dt);
                }

                mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xse-" + sIndexName + ".XSX";
            }
            else
                mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xse-h" + sIndexName + ".XSX";

            string sAxmlOutputPath = AppSettings.Get<string>("AxmlOutputPath");
            string filename = (sAxmlOutputPath + mAxmlFilename);

            if( IsFirstDate )
            {
                if (File.Exists(filename))
                    File.Delete(filename);

                using (StreamWriter file = new StreamWriter(filename))
                {
                    file.WriteLine("<?xml version=\"1.0\"?>");
                    file.WriteLine("<AdventXML version=\"3.0\">");
                    file.WriteLine(accountProvider);
                    file.WriteLine("<XSXList index=\"" + sIndexName + "\" date=\"" + DateHelper.ConvertToYYYYMMDD(sFileDate) + "\" " + batch);
                }
            }
            using (StreamWriter file = new StreamWriter(filename, true))
            {
                string weightAndIrr = "";
                string[] vals = null;
                if (addDummyEndOfMonthAxmlBefore)
                {
                    file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\">");
                    foreach (DictionaryEntry pair in sortedList)
                    {
                        weightAndIrr = pair.Value.ToString();
                        vals = weightAndIrr.Split('|');
                        file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + pair.Key + "\" weight=\"" + vals[0].ToString() + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                    file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\">");
                }
                else
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\">");
                }

                foreach (DictionaryEntry pair in sortedList)
                {
                    weightAndIrr = pair.Value.ToString();
                    vals = weightAndIrr.Split('|');
                    file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + pair.Key + "\" weight=\"" + vals[0].ToString() + "\" irr=\"" + vals[1].ToString() + "\"/>");
                }
                file.WriteLine("</XSXPeriod>");

                if (addDummyEndOfMonthAxmlAfter)
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" through=\"" + sEndOfMonthDate + "\" indexperfiso=\"usd\">");
                    foreach (DictionaryEntry pair in sortedList)
                    {
                        weightAndIrr = pair.Value.ToString();
                        vals = weightAndIrr.Split('|');
                        file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + pair.Key + "\" weight=\"" + vals[0].ToString() + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                }
            }

            if ( IsLastDate )
            {
                using (StreamWriter file = new StreamWriter(filename, true))
                {
                    file.WriteLine("</XSXList>");
                    file.WriteLine("</AccountProvider>");
                    file.Write("</AdventXML>");
                }
            }
        }

        public void GenerateAxmlFileSectors(string sBusinessDate, string sFileDate, string sIndexName, Vendors vendor,
            List<IndexRow> indexRowsSectorLevel1RollUp, List<IndexRow> indexRowsSectorLevel2RollUp, List<IndexRow> indexRowsSectorLevel3RollUp,
            bool IsFirstDate, bool IsLastDate)
        {
            List<IndexRow> indexRowsLevel4NotUsed = new List<IndexRow>();
            GenerateAxmlFileSectors(sBusinessDate, sFileDate, sIndexName, vendor, indexRowsSectorLevel1RollUp, indexRowsSectorLevel2RollUp, indexRowsSectorLevel3RollUp, indexRowsLevel4NotUsed,
                                    IsFirstDate, IsLastDate);
        }

        public void GenerateAxmlFileSectors(string sBusinessDate, string sFileDate, string sIndexName, Vendors vendor,
            List<IndexRow> indexRowsSectorLevel1RollUp, List<IndexRow> indexRowsSectorLevel2RollUp, List<IndexRow> indexRowsSectorLevel3RollUp, List<IndexRow> indexRowsSectorLevel4RollUp,
             bool IsFirstDate, bool IsLastDate)
        {

            string mAxmlFilename = "";
            bool addDummyEndOfMonthAxmlAfter = false;
            bool addDummyEndOfMonthAxmlBefore = DateHelper.IsPrevEndofMonthOnWeekend(sBusinessDate);

            DateTime prevEndofMonthDate = DateHelper.PrevEndOfMonthDay(sBusinessDate);
            string sPrevEndOfMonthDate = prevEndofMonthDate.ToString("yyyyMMdd");
            string sPrevBusinessDate = DateHelper.PrevBusinessDay(sBusinessDate);
            string sEndOfMonthDate = "";


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

            string prefix = "";
            string accountProvider = "";
            string batch = "";
            string level1Class = "";
            string level2Class = "";
            string level3Class = "";
            string level4Class = "";

            if (vendor.Equals(Vendors.Snp))
            {
                prefix = "ix-";
                accountProvider = "<AccountProvider name=\"StandardAndPoors\" code=\"ix\">";
                batch = "batch=\"7\">";
                level1Class = "class=\"GICSSector\"";
                level2Class = "class=\"GICSIndGrp\"";
                level3Class = "class=\"GICSIndustry\"";
                level4Class = "class=\"GICSSubInd\"";
            }
            else if (vendor.Equals(Vendors.Russell))
            {
                prefix = "rl-";
                accountProvider = "<AccountProvider name=\"Russell\" code=\"rl\">";
                batch = "batch=\"1\">";
                level1Class = "class=\"RGSSector\"";
                level2Class = "class=\"RGSSubSector\"";
                level3Class = "class=\"RGSIndustry\"";
            }

            if (IsFirstDate && IsLastDate)
            {
                addDummyEndOfMonthAxmlAfter = DateHelper.IsEndofMonthOnWeekend(sBusinessDate);

                if (addDummyEndOfMonthAxmlAfter)
                {
                    DateTime dt = DateHelper.EndOfMonthDay(sFileDate);
                    sFileDate = dt.ToString("MM/dd/yyyy");
                    sEndOfMonthDate = DateHelper.ConvertToYYYYMMDD(dt);
                }

                mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xnf-" + sIndexName + ".XNX";
            }
            else
                mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xnf-h" + sIndexName + ".XNX";

            string sAxmlOutputPath = AppSettings.Get<string>("AxmlOutputPath");
            string filename = (sAxmlOutputPath + mAxmlFilename);

            if (IsFirstDate)
            {
                if (File.Exists(filename))
                    File.Delete(filename);

                using (StreamWriter file = new StreamWriter(filename))
                {
                    file.WriteLine("<?xml version=\"1.0\"?>");
                    file.WriteLine("<AdventXML version=\"3.0\">");
                    file.WriteLine(accountProvider);
                    file.WriteLine("<XNXList index=\"" + sIndexName + "\" date=\"" + DateHelper.ConvertToYYYYMMDD(sFileDate) + "\" " + batch);
                }
            }

            using (StreamWriter file = new StreamWriter(filename, true))
            {
                /////////////////////////////////////////// Level 1
                if (addDummyEndOfMonthAxmlBefore)
                {
                    file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\" " + level1Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel1RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel1 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                    file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level1Class + "> ");
                }
                else
                {
                    file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level1Class + "> ");
                }

                foreach (IndexRow indexRow in indexRowsSectorLevel1RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel1 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                if (addDummyEndOfMonthAxmlAfter)
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" through=\"" + sEndOfMonthDate + "\" indexperfiso=\"usd\" " + level1Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel1RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel1 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                }

                /////////////////////////////////////////// Level 2
                if (addDummyEndOfMonthAxmlBefore)
                {
                    file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\" " + level2Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel2RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel2 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                    file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level2Class + "> ");
                }
                else
                {
                    file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level2Class + "> ");
                }

                foreach (IndexRow indexRow in indexRowsSectorLevel2RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel2 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                if (addDummyEndOfMonthAxmlAfter)
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" through=\"" + sEndOfMonthDate + "\" indexperfiso=\"usd\" " + level2Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel2RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel2 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                }

                /////////////////////////////////////////// Level 3               
                if (addDummyEndOfMonthAxmlBefore)
                {
                    file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\" " + level3Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel3RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel3 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                    file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level3Class + "> ");
                }
                else
                {
                    file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level3Class + "> ");
                }

                foreach (IndexRow indexRow in indexRowsSectorLevel3RollUp)
                {
                    file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel3 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }
                file.WriteLine("</XNXPeriod>");

                if (addDummyEndOfMonthAxmlAfter)
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" through=\"" + sEndOfMonthDate + "\" indexperfiso=\"usd\" " + level3Class + "> ");
                    foreach (IndexRow indexRow in indexRowsSectorLevel3RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel3 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                }

                /////////////////////////////////////////// Level 4
                if (vendor.Equals(Vendors.Snp))
                {

                    if (addDummyEndOfMonthAxmlBefore)
                    {
                        file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\" " + level4Class + "> ");
                        foreach (IndexRow indexRow in indexRowsSectorLevel4RollUp)
                        {
                            file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel4 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                        }
                        file.WriteLine("</XSXPeriod>");
                        file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level4Class + "> ");
                    }
                    else
                    {
                        file.WriteLine("<XNXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\" " + level4Class + "> ");
                    }

                    foreach (IndexRow indexRow in indexRowsSectorLevel4RollUp)
                    {
                        file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel4 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                    }
                    file.WriteLine("</XNXPeriod>");

                    if (addDummyEndOfMonthAxmlAfter)
                    {
                        file.WriteLine("<XSXPeriod from=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" through=\"" + sEndOfMonthDate + "\" indexperfiso=\"usd\" " + level4Class + "> ");
                        foreach (IndexRow indexRow in indexRowsSectorLevel4RollUp)
                        {
                            file.WriteLine("<XNXDetail code=\"" + indexRow.SectorLevel4 + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                        }
                        file.WriteLine("</XSXPeriod>");
                    }
                }
            }
            if (IsLastDate)
            {
                using (StreamWriter file = new StreamWriter(filename, true))
                {
                    file.WriteLine("</XNXList>");
                    file.WriteLine("</AccountProvider>");
                    file.Write("</AdventXML>");
                }
            }
        }

        public void CopyFilesToFtpFolder(string sFileDate, Vendors vendor, string sIndexName, AdventOutputType outputType)
        {

        }


        public void CopyFileToFtpFolder(string clientId, string sFileDate, Vendors vendor, string sIndexName, AdventOutputType outputType)
        {
            try
            {
                string ftpRootDir = AppSettings.Get<string>("ftpRootDir");
                string sourceDir = AppSettings.Get<string>("AxmlOutputPath");
                string destDir = ftpRootDir + "\\" + clientId + "\\IndexData\\Results\\";

                if (Directory.Exists(sourceDir) && Directory.Exists(destDir))
                {
                    bool bEndOfMonthOnWeekend = DateHelper.IsEndofMonthOnWeekend(sFileDate);

                    if (bEndOfMonthOnWeekend)
                    {
                        DateTime dt = DateHelper.EndOfMonthDay(sFileDate);
                        sFileDate = dt.ToString("MM/dd/yyyy");
                    }

                    string prefix = "";
                    string sourceFilename = "";

                    if (vendor.Equals(Vendors.Snp))
                    {
                        prefix = "ix-";
                    }
                    else if (vendor.Equals(Vendors.Russell))
                    {
                        prefix = "rl-";
                    }
                    if (outputType.Equals(AdventOutputType.Constituent))
                    {
                        sourceFilename = sourceDir + prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xse-" + sIndexName + ".XSX";
                    }
                    else if (outputType.Equals(AdventOutputType.Sector))
                    {
                        sourceFilename = sourceDir + prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xnf-" + sIndexName + ".XNX";
                    }
                    if (File.Exists(sourceFilename))
                    {
                        File.Copy(sourceFilename, destDir + Path.GetFileName(sourceFilename), true);
                    }
                }
            }
            catch
            {

            }
            finally
            {

            }
        }


        public void VendorDatasetJobsUpdateProcessDate(string Vendor, string Dataset, string sProcessDate)
        {
            SqlCommand cmd = null;
            string logFuncName = "VendorDatasetJobsUpdateProcessDate: ";

            string commandText = @"
                update Jobs set LastProcessDate = @ProcessDate
                WHERE  Vendor = @Vendor and DataSet = @Dataset and JobType = 'Vendor' and Active = 'Yes'
                ";
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionStringIndexData);
                    mSqlConn.Open();
                }

                cmd = new SqlCommand
                {
                    Connection = mSqlConn,
                    CommandText = commandText
                };

                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters["@Vendor"].Value = Vendor;
                cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                cmd.Parameters["@Dataset"].Value = Dataset;
                cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;

                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                LogHelper.WriteLine(logFuncName + " done ");
            }
        }
    }
}
