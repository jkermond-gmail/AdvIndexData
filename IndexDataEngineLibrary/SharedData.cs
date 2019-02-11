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
                    Console.WriteLine(ex.Message);
                }
            }
            finally
            {
            }

        }

        /*
<?xml version="1.0"?>
<AdventXML version="3.0">
<AccountProvider name="Russell" code="rl">
<XSXList index="r1000v" date="20180402" batch="6">
<XSXPeriod from="20180329" through="20180331" indexperfiso="usd">
<XSXDetail type="cs" iso="usd" symbol="a" weight="0.135035463" irr="0"/>
<XSXDetail type="cs" iso="usd" symbol="aa" weight="0.068634999" irr="0"/>
.
.
<XSXDetail type="cs" iso="usd" symbol="zion" weight="0.085081808" irr="0"/>
<XSXDetail type="cs" iso="usd" symbol="znga" weight="0.02324712" irr="0"/>
</XSXPeriod>
<XSXPeriod from="20180331" through="20180402" indexperfiso="usd">
<XSXDetail type="cs" iso="usd" symbol="a" weight="0.135035463" irr="-3.469003739"/>
<XSXDetail type="cs" iso="usd" symbol="aa" weight="0.068634999" irr="-1.379007356"/>

.
.
<XSXDetail type="cs" iso="usd" symbol="zion" weight="0.085081808" irr="-2.579005934"/>
<XSXDetail type="cs" iso="usd" symbol="znga" weight="0.02324712" irr="-3.005021719"/>
</XSXPeriod>
</XSXList>
</AccountProvider>
</AdventXML>
         */

        public void GenerateAxmlFileConstituents(string sBusinessDate, string sFileDate, string sIndexName, Vendors vendor, List<IndexRow> indexRowsTickerSort, bool IsFirstDate, bool IsLastDate)
        {
            string mAxmlFilename = "";
            bool addDummyEndOfMonthAXML = DateHelper.IsPrevEndofMonthOnWeekend(sBusinessDate);

            DateTime endofMonthDate = DateHelper.PrevEndOfMonthDay(sBusinessDate);
            string sPrevEndOfMonthDate = endofMonthDate.ToString("yyyyMMdd");
            string sPrevBusinessDate = DateHelper.PrevBusinessDay(sBusinessDate);

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

            mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xse-" + sIndexName + ".XSX";
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
                if (addDummyEndOfMonthAXML)
                {
                    file.WriteLine("<XSXPeriod from=\"" + sPrevBusinessDate + "\" through=\"" + sPrevEndOfMonthDate + "\" indexperfiso=\"usd\">");
                    foreach (IndexRow indexRow in indexRowsTickerSort)
                    {
                        file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + indexRow.Ticker + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + "0" + "\"/>");
                    }
                    file.WriteLine("</XSXPeriod>");
                    file.WriteLine("<XSXPeriod from=\"" + sPrevEndOfMonthDate + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\">");
                }
                else
                {
                    file.WriteLine("<XSXPeriod from=\"" + DateHelper.PrevBusinessDay(sBusinessDate) + "\" through=\"" + DateHelper.ConvertToYYYYMMDD(sBusinessDate) + "\" indexperfiso=\"usd\">");
                }

                foreach (IndexRow indexRow in indexRowsTickerSort)
                {
                    file.WriteLine("<XSXDetail type=\"cs\" iso=\"usd\" symbol=\"" + indexRow.Ticker + "\" weight=\"" + indexRow.Weight.ToString(NumberFormat, mCultureInfo) + "\" irr=\"" + indexRow.RateOfReturn.ToString(NumberFormat, mCultureInfo) + "\"/>");
                }

                file.WriteLine("</XSXPeriod>");
            }

            if( IsLastDate )
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
            bool addDummyEndOfMonthAXML = DateHelper.IsPrevEndofMonthOnWeekend(sBusinessDate);

            DateTime endofMonthDate = DateHelper.PrevEndOfMonthDay(sBusinessDate);
            string sPrevEndOfMonthDate = endofMonthDate.ToString("yyyyMMdd");
            string sPrevBusinessDate = DateHelper.PrevBusinessDay(sBusinessDate);

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

            mAxmlFilename = prefix + DateHelper.ConvertToYYYYMMDD(sFileDate) + "-xnf-" + sIndexName + ".XNX";
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
                if (addDummyEndOfMonthAXML)
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
                /////////////////////////////////////////// Level 2

                if (addDummyEndOfMonthAXML)
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
                /////////////////////////////////////////// Level 3
                
                if (addDummyEndOfMonthAXML)
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
                /////////////////////////////////////////// Level 4

                if (vendor.Equals(Vendors.Snp))
                {

                    if (addDummyEndOfMonthAXML)
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
    }
}
