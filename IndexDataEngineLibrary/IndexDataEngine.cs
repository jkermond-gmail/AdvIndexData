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
using System.Configuration;


namespace IndexDataEngineLibrary
{
    public class IndexDataEngine
    {
        private LogHelper logHelper;

        private string sConnectionIndexData = null;
        private string sConnectionAmdVifs = null;

        private SqlConnection cnSqlIndexData = null;
        private SqlConnection cnSqlAmdVifs = null;
        private string sAmdVifsProcessDate ;
        private DateTime AmdVifsProcessDate;
        private string sIndexDataProcessDate;
        private DateTime IndexDataProcessDate;

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
            sConnectionIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            sConnectionAmdVifs = ConfigurationManager.ConnectionStrings["dbConnectionAmdVifs"].ConnectionString;

            BeginSql();

            sAmdVifsProcessDate = getVIFsProcessDate();
            AmdVifsProcessDate = DateTime.ParseExact(sAmdVifsProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            sIndexDataProcessDate = getIndexDataProcessDate();
            IndexDataProcessDate = DateTime.ParseExact(sIndexDataProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);

            if( AmdVifsProcessDate > IndexDataProcessDate)
            {
                // Initialize everything cuz its a new day
                setIndexDataProcessDate(sAmdVifsProcessDate);
            }
            ProcessIndexDataWork();
            //RussellData russellData = new RussellData(logHelper);
            //russellData.SetConnectionString(sConnectionIndexData);
            //DateTime StartDate = DateTime.ParseExact("01/03/2017", "MM/dd/yyyy", CultureInfo.InvariantCulture);
            //DateTime EndDate = StartDate.AddDays(1.0);
            //russellData.ProcessRussellHoldingsFiles(StartDate, EndDate, true, true);

            //EndSql();

        }

        private void ProcessIndexDataWork()
        {

        }

        private void BeginSql()
        {
            cnSqlIndexData = new SqlConnection(sConnectionIndexData);
            cnSqlIndexData.Open();
            cnSqlAmdVifs = new SqlConnection(sConnectionAmdVifs);
            cnSqlAmdVifs.Open();
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
        //private string VIFLastProcessDateOld()
        //{
        //        return(getSystemSettingValue("VIFLastProcessDate"));
        //}
        


        private string getSystemSettingValue(string SettingName, SqlConnection sqlConnection)
        {

            //string sSettingValue = "";
            //string SqlSelect = @"
            //        SELECT SettingValue 
            //        FROM SystemSettings 
            //        WHERE SettingName = @SettingName
            //        ";
            //string sColumn = "SettingValue";
            //using (AdoHelper db = new AdoHelper(sConnection))
            //using (SqlDataReader dr = db.ExecDataReader(SqlSelect, "@SettingName", SettingName))
            //{
            //    sSettingValue = db.ReadDataReader(dr, sColumn);
            //    dr.Close();
            //}
            //return (sSettingValue);



            string SettingValue = "";
            SqlDataReader dr1 = null;

            try
            {
                string SqlSelect = @"
                    SELECT SettingValue 
                    FROM SystemSettings 
                    WHERE SettingName = @SettingName
                    ";

                SqlCommand cmd1 = new SqlCommand(SqlSelect, sqlConnection);
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
            }

            return (SettingValue);

        }

        public static void setSystemSettingValue(string SettingName, string SettingValue, SqlConnection sqlConnection)
        {


            SqlCommand cmd = null;

            try
            {
                cmd = new SqlCommand
                {
                    Connection = sqlConnection,
                    CommandText =
                        "update SystemSettings set SettingValue = '" + SettingValue.ToString() + "' WHERE SettingName = '" + SettingName.ToString() + "'"
                };

                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                Console.WriteLine(ex.Message);
                //LogHelper.WriteLine(logFuncName + " " + colName + " " + ex.Message);
            }
            finally
            {
                //LogHelper.WriteLine(logFuncName + "Rows Updated " + updateCount + " " + colName);
            }
        }




        private string getVIFsProcessDate()
        {
            string sDate = "";
            sDate = getSystemSettingValue("VIFLastProcessDate", cnSqlAmdVifs);
            return (sDate);
        }

        private string getIndexDataProcessDate()
        {
            string sDate = "";
            sDate = getSystemSettingValue("IndexDataProcessDate", cnSqlIndexData);
            return (sDate);
        }

        private void setIndexDataProcessDate(string sDate)
        {
            setSystemSettingValue("IndexDataProcessDate", sDate, cnSqlIndexData);
        }



        private void EndSql()
        {
            cnSqlIndexData.Close();
            cnSqlAmdVifs.Close();
        }


    }
}
