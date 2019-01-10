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


        public void CompareAxmlForDateRange(string Vendor, string OutputType, string Indexname, string sStartDate, string sEndDate)
        {
            DateTime startDate = Convert.ToDateTime(sStartDate);
            DateTime endDate = Convert.ToDateTime(sEndDate);
            DateTime returnDate;
            int DateCompare;

            for (returnDate = startDate
            ; (DateCompare = returnDate.CompareTo(endDate)) <= 0
            ; returnDate = DateHelper.NextBusinessDay(returnDate))
            {
                string IndexnameNotUsed = "";
                DateTime ReturnDateNotUsed = DateTime.MinValue;
                string Source = "";
                string AxmlFilename = "";
                string prefix = "";

                if (Vendor.Equals("Russell"))
                    prefix = "rl";
                else if (Vendor.Equals("Snp"))
                    prefix = "ix";

                if (OutputType.Equals("Constituent"))
                    //Indexfilename = @"rl-20181231-xse-r3000.XSX";                        
                    AxmlFilename = prefix + "-" + DateHelper.ConvertToYYYYMMDD(returnDate.ToShortDateString()) + "-xse-" + Indexname + ".XSX";
                else if (OutputType.Equals("Sector"))
                    //Indexfilename = @"rl-20180103-xnf-r1000.XNX";
                    AxmlFilename = prefix + "-" + DateHelper.ConvertToYYYYMMDD(returnDate.ToShortDateString()) + "-xnf-" + Indexname + ".XNX";

                string sAxmlFilePathDev = AppSettings.Get<string>("AxmlOutputPath");
                sAxmlFilePathDev = sAxmlFilePathDev + AxmlFilename;
                string sAxmlFilePathProd = AppSettings.Get<string>("AxmlOutputPathProd");
                sAxmlFilePathProd = sAxmlFilePathProd + AxmlFilename;

                if( File.Exists(sAxmlFilePathDev) && File.Exists(sAxmlFilePathProd))
                {
                    LogHelper.WriteLine("CompareAxmlForDateRange: " + sAxmlFilePathDev + " " + sAxmlFilePathProd);
                    Source = "Dev";
                    if (OutputType.Equals("Constituent"))
                        AddAxmlConstituentData(sAxmlFilePathDev, Source, out IndexnameNotUsed, out ReturnDateNotUsed, out OutputType);
                    else if (OutputType.Equals("Sector"))
                        AddAxmlSectorData(sAxmlFilePathDev, Source, out IndexnameNotUsed, out ReturnDateNotUsed, out OutputType);

                    Source = "Prod";
                    if (OutputType.Equals("Constituent"))
                        AddAxmlConstituentData(sAxmlFilePathProd, Source, out IndexnameNotUsed, out ReturnDateNotUsed, out OutputType);
                    else if (OutputType.Equals("Sector"))
                        AddAxmlSectorData(sAxmlFilePathProd, Source, out IndexnameNotUsed, out ReturnDateNotUsed, out OutputType);

                    CompareAxmlOutput(Indexname, returnDate, OutputType);
                }
                else
                {
                    if( !File.Exists(sAxmlFilePathDev))
                        LogHelper.WriteLine("CompareAxmlForDateRange: missing dev  file " + sAxmlFilePathDev);

                    if ( !File.Exists(sAxmlFilePathProd))
                        LogHelper.WriteLine("CompareAxmlForDateRange: missing prod file " + sAxmlFilePathProd);
                }
            }
        }


        public void RunCompare()
        {
            /*
             For a given date
                For each vendor
                    For each output type
                        For each index
                            Add AxmlData Dev
                            Add AxmlData Prod
                            Compare AxmlData
                                Do securities/sectors match?
                                Do weights match?
                                Do Irrs match?
                                Do they roll up to the same value? and does valeu match published vendor Total Return?

             */ 
            string Source = "";
            string Indexfilename = @"";
            string IndexName = "";
            DateTime ReturnDate = DateTime.MinValue;
            string OutputType = "";

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\ix-20181231-xse-sp500.XSX";
            AddAxmlConstituentData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\ix-20181231-xse-sp500.XSX";
            AddAxmlConstituentData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            CompareAxmlOutput(IndexName, ReturnDate, OutputType);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\ix-20181231-xnf-sp500.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\ix-20181231-xnf-sp500.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            CompareAxmlOutput(IndexName, ReturnDate, OutputType);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\rl-20181231-xse-r3000.XSX";
            AddAxmlConstituentData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\rl-20181231-xse-r3000.XSX";
            AddAxmlConstituentData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            CompareAxmlOutput(IndexName, ReturnDate, OutputType);

            Source = "Dev";
            Indexfilename = @"C:\IndexData\AxmlOutputDev\rl-20181231-xnf-r3000.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            Source = "Prod";
            Indexfilename = @"C:\IndexData\AxmlOutputProd\rl-20181231-xnf-r3000.XNX";
            AddAxmlSectorData(Indexfilename, Source, out IndexName, out ReturnDate, out OutputType);

            CompareAxmlOutput(IndexName, ReturnDate, OutputType);
        }

        public void AddAxmlSectorData(string FileName, string Source, out string IndexName, out DateTime ReturnDate, out string OutputType)
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");

            IndexName = "";
            ReturnDate = DateTime.MinValue;
            OutputType = "Sector";
            string OutputSubType = "";
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
                        OutputSubType = Split[7];
                    }
                }

                if (FoundIndexName && FoundReturnDate && TextLine.Contains(AXML_DETAIL_TAG_XNX))
                {
                    Split = TextLine.Split('\"');
                    Identifier = Split[1];
                    Weight = Split[3];
                    IRR = Split[5];
                    AddAxmlDailyOutput(Indexname, ReturnDate, OutputType, OutputSubType, Source, Identifier, Weight, IRR, AddCount);
                    AddCount += 1;
                }

                if (FoundReturnDate && TextLine.Contains(AXML_PERIOD_END_TAG_XNX))
                {
                    FoundReturnDate = false;
                }

            }
            srAxmlFile.Close();
        }


        public void AddAxmlConstituentData(string FileName, string Source, out string IndexName, out DateTime ReturnDate, out string OutputType )
        {

            string TextLine;
            CultureInfo enUS = new CultureInfo("en-US");
            string OutputSubType = "";
            IndexName = "";
            ReturnDate = DateTime.MinValue;
            OutputType = "Constituent";
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
                    AddAxmlDailyOutput(IndexName, ReturnDate, OutputType, OutputSubType, Source, Ticker, Weight, IRR, AddCount);
                    AddCount += 1;
                }
            }
            srAxmlFile.Close();
        }

        public void AddAxmlDailyOutput(
            string IndexName,
            DateTime ReturnDate,
            string OutputType,
            string OutputSubType,
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
                    OutputType = @OutputType and
                    Source = @Source
                    ";

                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@OutputType", SqlDbType.VarChar);
                cmd.Parameters.Add("@OutputSubType", SqlDbType.VarChar);
                cmd.Parameters.Add("@Source", SqlDbType.VarChar);
                cmd.Parameters.Add("@Identifier", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = IndexName;
                cmd.Parameters["@ReturnDate"].Value = ReturnDate;
                cmd.Parameters["@OutputType"].Value = OutputType;
                cmd.Parameters["@OutputSubType"].Value = OutputSubType;
                cmd.Parameters["@Source"].Value = Source;
                cmd.Parameters["@Identifier"].Value = Identifier;

                if (AddCount.Equals(0))
                    cmd.ExecuteNonQuery();

                cmd.CommandText = @"
                    select count(*) from AxmlOutput where 
                    IndexName = @IndexName and 
                    ReturnDate = @ReturnDate and 
                    OutputType = @OutputType and
                    Source = @Source and
                    Identifier = @Identifier                    
                    ";

                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText = @"
                        insert into AxmlOutput
                        (IndexName, ReturnDate, OutputType, OutputSubType, Source, Identifier, Weight, IRR) Values
                        (@IndexName, @ReturnDate, @OutputType, @OutputSubType, @Source, @Identifier, @Weight, @IRR)
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

        public void CompareAxmlOutput(string IndexName, DateTime ReturnDate, string OutputType)
        {
            if ( CompareAxmlIdentifiers(IndexName, ReturnDate, OutputType) == true )
            {
                CompareAxmlWeights(IndexName, ReturnDate, OutputType);
                CompareAxmlReturns(IndexName, ReturnDate, OutputType);
            }
        }

        private bool CompareAxmlIdentifiers(string IndexName, DateTime ReturnDate, string OutputType)
        {
            bool compare1 = false;
            bool compare2 = false;
            string Identifier = "";
            string Source1 = "";
            string Source2 = "";
            /*
            SELECT Identifier
            FROM AxmlOutput
            where Source = 'Dev' and OutputType = 'Security' and IndexName = 'r3000'
            and Identifier not in (
            SELECT Identifier
            FROM AxmlOutput
            where Source = 'Prod' and OutputType = 'Security' and IndexName = 'r3000'
            )
            */

            LogHelper.WriteLine("CompareAxmlIdentifiers: " + IndexName + " " + ReturnDate.ToShortDateString() + " " + OutputType);

            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(sharedData.ConnectionStringIndexData);
                    mSqlConn.Open();
                }

                for (int i = 1; i <= 2; i++)
                {
                    if( i.Equals(1))
                    {
                        Source1 = "Dev";
                        Source2 = "Prod";
                    }
                    else if( i.Equals(2))
                    {
                        Source1 = "Prod";
                        Source2 = "Dev";
                    }

                    string SqlSelect = @"
                    SELECT Identifier
                    FROM AxmlOutput
                    where Source = @Source1 and OutputType = @OutputType 
                    and IndexName = @IndexName and ReturnDate = @ReturnDate
                    and Identifier not in (
                        SELECT Identifier
                        FROM AxmlOutput
                        where Source = @Source2 and OutputType = @OutputType 
                        and IndexName = @IndexName and ReturnDate = @ReturnDate
                    )
                    ";

                    SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                    cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                    cmd.Parameters.Add("@OutputType", SqlDbType.VarChar);
                    cmd.Parameters.Add("@Source1", SqlDbType.VarChar);
                    cmd.Parameters.Add("@Source2", SqlDbType.VarChar);
                    cmd.Parameters["@IndexName"].Value = IndexName;
                    cmd.Parameters["@ReturnDate"].Value = ReturnDate;
                    cmd.Parameters["@OutputType"].Value = OutputType;
                    cmd.Parameters["@Source1"].Value = Source1;
                    cmd.Parameters["@Source2"].Value = Source2;

                    SqlDataReader dr = null;
                    dr = cmd.ExecuteReader();
                    int rows = 0;
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            rows += 1;
                            Identifier = dr["Identifier"].ToString();
                            LogHelper.WriteLine( Source1 + " Identifier " + Identifier + " missing from " + Source2);
                        }
                    }
                    else
                    {
                        LogHelper.WriteLine("No " + Source1 + " Identifiers missing from " + Source2);
                        if (i.Equals(1))
                        {
                            compare1 = true;
                        }
                        else if (i.Equals(2))
                        {
                            compare2 = true;
                        }
                    }
                    dr.Close();
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

            if (compare1.Equals(true) || compare2.Equals(true))
                return (true);
            else
                return (false);

        }

        private bool CompareAxmlWeights(string IndexName, DateTime ReturnDate, string OutputType)
        {
            bool compare = false;

            return (compare);

        }
        private bool CompareAxmlReturns(string IndexName, DateTime ReturnDate, string OutputType)
        {
            bool compare = false;

            return (compare);

        }

    }
}
