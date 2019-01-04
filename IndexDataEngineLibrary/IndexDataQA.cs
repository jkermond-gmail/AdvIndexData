using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Data;
using System.Globalization;
using AdventUtilityLibrary;


namespace IndexDataEngineLibrary
{
    public sealed class IndexDataQA
    {
        private SqlConnection mSqlConn = null;
        private SharedData sharedData = null;


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

        public IndexDataQA()
        {
            //dateHelper = 
            //LogHelper.Info("RussellData()", "RussellData");
            sharedData = new SharedData();
        }


        public void RunCompare()
        {
            string Source = "";
            string Indexfilename = @"";
            string IndexName = "";
            DateTime ReturnDate = DateTime.MinValue;
            string VendorFormat = "";

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\ix-20181231-xse-sp500.XSX";
            AddAxmlSecurityData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\ix-20181231-xse-sp500.XSX";
            AddAxmlSecurityData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\ix-20181231-xnf-sp500.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\ix-20181231-xnf-sp500.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\rl-20181231-xse-r3000.XSX";
            AddAxmlSecurityData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\rl-20181231-xse-r3000.XSX";
            AddAxmlSecurityData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\rl-20181231-xnf-r3000.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\rl-20181231-xnf-r3000.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out VendorFormat);


        }

        public void AddAxmlSectorData(string FileName, string Source, out string IndexName, out DateTime ReturnDate, out string VendorFormat)
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            IndexName = "";
            ReturnDate = DateTime.MinValue;
            VendorFormat = "";
            
            string Identifier = "";
            string Indexname = "";
            string IRR = "";
            string Weight = "";
            StreamReader srAxmlFile = null;
            bool FoundIndexName = false;
            bool FoundReturnDate = false;
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
                    FoundIndexName = TextLine.Contains(AXML_LIST_TAG_XNX);
                    if (FoundIndexName)
                    {
                        Split = TextLine.Split('\"');
                        Split = TextLine.Split('"');
                        Indexname = Split[1];
                    }
                }

                if (!FoundReturnDate)
                {
                    FoundReturnDate = TextLine.Contains(AXML_PERIOD_BEGIN_TAG_XNX);
                    if (FoundReturnDate)
                    {
                        Split = TextLine.Split('\"');
                        string sDate = Split[3];
                        DateTime.TryParseExact(sDate, "yyyyMMdd", enUS, DateTimeStyles.None, out ReturnDate);
                        VendorFormat = Split[7];
                    }
                }

                if (FoundIndexName && FoundReturnDate && TextLine.Contains(AXML_DETAIL_TAG_XNX))
                {
                    Split = TextLine.Split('\"');
                    Identifier = Split[1];
                    Weight = Split[3];
                    IRR = Split[5];
                    AddAxmlDailyOutput(Indexname, ReturnDate, VendorFormat, Source, Identifier, Weight, IRR, AddCount);
                    AddCount += 1;
                }

                if (FoundReturnDate && TextLine.Contains(AXML_PERIOD_END_TAG_XNX))
                {
                    FoundReturnDate = false;
                }

            }
            srAxmlFile.Close();
        }


        public void AddAxmlSecurityData(string FileName, string Source, out string IndexName, out DateTime ReturnDate, out string VendorFormat )
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            IndexName = "";
            ReturnDate = DateTime.MinValue;
            VendorFormat = "";
            string Ticker = "";
            string IRR = "";
            string Weight = "";
            StreamReader srAxmlFile = null;
            bool FoundIndexName = false;
            bool FoundReturnDate = false;
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
                        IndexName = Split[1];
                    }
                }

                if (!FoundReturnDate)
                {
                    FoundReturnDate = TextLine.Contains(AXML_PERIOD_BEGIN_TAG_XSX);
                    if (FoundReturnDate)
                    {
                        Split = TextLine.Split('\"');
                        string sDate = Split[3];
                        DateTime.TryParseExact(sDate, "yyyyMMdd", enUS, DateTimeStyles.None, out ReturnDate);
                    }
                }

                if (FoundIndexName && FoundReturnDate && TextLine.Contains(AXML_DETAIL_TAG_XSX))
                {
                    Split = TextLine.Split('\"');
                    Ticker = Split[5];
                    Weight = Split[7];
                    IRR = Split[9];
                    VendorFormat = "Security";
                    AddAxmlDailyOutput(IndexName, ReturnDate, VendorFormat, Source, Ticker, Weight, IRR, AddCount);
                    AddCount += 1;
                }
            }
            srAxmlFile.Close();
        }

        public void AddAxmlDailyOutput(
            string IndexName,
            DateTime ReturnDate,
            string VendorFormat,
            string Source,
            string Identifier,
            string Weight,
            string IRR,
            int AddCount
            )
                {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                string SqlSelect = @"
                    delete from AxmlOutput where 
                    IndexName = @IndexName and 
                    ReturnDate = @ReturnDate and 
                    VendorFormat = @VendorFormat and
                    Source = @Source
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

                if (AddCount.Equals(0))
                    cmd.ExecuteNonQuery();

                cmd.CommandText = @"
                    select count(*) from AxmlOutput where 
                    IndexName = @IndexName and 
                    ReturnDate = @ReturnDate and 
                    VendorFormat = @VendorFormat and
                    Source = @Source and
                    Identifier = @Identifier                    
                    ";

                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText = @"
                        insert into AxmlOutput
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
    }
}
