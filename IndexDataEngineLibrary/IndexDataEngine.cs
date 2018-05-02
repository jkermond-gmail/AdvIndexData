using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using AdventUtilityLibrary;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;


namespace IndexDataEngineLibrary
{
    public class IndexDataEngine
    {
        private LogHelper logHelper;
        //private string sConnectionIndexData = "server=VSTGMDDB2-1;database=IndexData;uid=sa;pwd=M@gichat!";
        //            //connectionString = ConfigurationManager.AppSettings["AdoConnectionString"];

        private string sConnectionIndexData = @"server=JKERMOND-NEW\SQLEXPRESS2014;database=IndexData;uid=sa;pwd=M@gichat!";

        //private string sConnectionAmdVifsDB = "server=VSTGMDDB2-1;database=AmdVifsDB;uid=sa;pwd=M@gichat!";
        private string sConnectionAmdVifsDB = @"server=JKERMOND-NEW\SQLEXPRESS2014;database=AmdVifsDB;uid=sa;pwd=M@gichat!";

        private SqlConnection cnSqlIndexData = null;
        private SqlConnection cnSqlAmdVifsDB = null;
        //private string sProcessDate ;
        //private DateTime ProcessDate;

        public IndexDataEngine()
        {
            Trace.WriteLine("IndexDataEngine()");
        }

        public IndexDataEngine( LogHelper appLogHelper)
        {
            logHelper = appLogHelper;
            logHelper.Info("IndexDataEngine()", "IndexDataEngineLibrary");

        }


        public void Run()
        {
            logHelper.Info("IndexDataEngine.Run", "IndexDataEngineLibrary");
            //BeginSql();

            //sProcessDate = VIFLastProcessDate();

            //ProcessDate = DateTime.ParseExact(sProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            //RussellData russellData = new RussellData(logHelper);
            //russellData.SetConnectionString(sConnectionIndexData);
            //DateTime StartDate = DateTime.ParseExact("01/03/2017", "MM/dd/yyyy", CultureInfo.InvariantCulture);
            //DateTime EndDate = StartDate.AddDays(1.0);
            //russellData.ProcessRussellHoldingsFiles(StartDate, EndDate, true, true);

            //EndSql();

        }

        private void BeginSql()
        {
            cnSqlIndexData = new SqlConnection(sConnectionIndexData);
            cnSqlIndexData.Open();
            cnSqlAmdVifsDB = new SqlConnection(sConnectionAmdVifsDB);
            cnSqlAmdVifsDB.Open();
            /*
             * https://www.codeproject.com/Tips/555870/SQL-Helper-Class-Microsoft-NET-Utility
             * http://www.blackbeltcoder.com/Articles/ado/an-ado-net-sql-helper-class
             * http://stackoverflow.com/questions/1221406/any-decent-ado-net-helper-utils-out-there
             * http://codereview.stackexchange.com/questions/63480/simple-sqlhelper-which-wraps-ado-net-methods
             * https://www.mssqltips.com/sqlservertip/3009/how-to-create-an-adonet-data-access-utility-class-for-sql-server/
             * https://msdn.microsoft.com/en-us/library/dn440726(v=pandp.60).aspx
             * https://msdn.microsoft.com/en-us/library/jj943772.aspx
             * 
             */
        }

        /// <summary>
        /// getVIFLastProcessDate
        /// </summary>
        /// <returns></returns>
        private string VIFLastProcessDateOld()
        {
                return(getSystemSettingValue("VIFLastProcessDate"));
        }
        


        private string getSystemSettingValue(string SettingName)
        {

            string sSettingValue = "";
            string SqlSelect = @"
                    SELECT SettingValue 
                    FROM SystemSettings 
                    WHERE SettingName = @SettingName
                    ";
            string sColumn = "SettingValue";
            using (AdoHelper db = new AdoHelper(sConnectionAmdVifsDB))
            using (SqlDataReader dr = db.ExecDataReader(SqlSelect, "@SettingName", SettingName))
            {
                sSettingValue = db.ReadDataReader(dr, sColumn);
                dr.Close();
            }
            return (sSettingValue);


            /*
            string SettingValue = "";
            SqlDataReader dr1 = null;

            try
            {
                string SqlSelect = @"
                    SELECT SettingValue 
                    FROM SystemSettings 
                    WHERE SettingName = @SettingName
                    ";

                SqlCommand cmd1 = new SqlCommand(SqlSelect, cnSqlAmdVifsDB);
                cmd1.Parameters.Add("@SettingName", SqlDbType.VarChar);
                cmd1.Parameters["@SettingName"].Value = SettingName;
                dr1 = cmd1.ExecuteReader();
                if (dr1.HasRows)
                {
                    if (dr1.Read())
                    {
                        SettingValue = dr1["SettingValue"].ToString();
                    }
                }
                dr1.Close();
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
                dr1.Close();
                //cnSqlAmdVifsDB.Close();
            }

            return (SettingValue);
            */
        }

        public static void setSystemSettingValue(string SettingName, string SettingValue)
        {
            string sSql = "update SystemSettings set SettingValue = '" +
                SettingValue.ToString() + "' WHERE SettingName = '" + SettingName.ToString() + "'";
        }


        private string VIFLastProcessDate()
        {
            string sDate = "";
            sDate = getSystemSettingValue("VIFLastProcessDate");
            return (sDate);
        }



        private void EndSql()
        {
            cnSqlIndexData.Close();
            cnSqlAmdVifsDB.Close();
        }


    }
}
