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
        private string sAmdVifsProcessDate;
        private DateTime AmdVifsProcessDate;
        private string sIndexDataProcessDate;
        private DateTime IndexDataProcessDate;

        public IndexDataEngine()
        {
            Trace.WriteLine("IndexDataEngine()");
        }

        public IndexDataEngine(LogHelper appLogHelper)
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

            if (AmdVifsProcessDate > IndexDataProcessDate)
            {
                // Initialize everything cuz its a new day
                setIndexDataProcessDate(sAmdVifsProcessDate);
            }
            ProcessIndexDataWork(sAmdVifsProcessDate);
            //RussellData russellData = new RussellData(logHelper);
            //russellData.SetConnectionString(sConnectionIndexData);
            //DateTime StartDate = DateTime.ParseExact("01/03/2017", "MM/dd/yyyy", CultureInfo.InvariantCulture);
            //DateTime EndDate = StartDate.AddDays(1.0);
            //russellData.ProcessRussellHoldingsFiles(StartDate, EndDate, true, true);

            //EndSql();

        }

        private void ProcessIndexDataWork(string sProcessDate)
        {
            //List<IndexRow> indexRowsTickerSort = new List<IndexRow>();

            //var list = new List<KeyValuePair<string, int>>();
            //list.Add(new KeyValuePair<string, int>("Cat", 1));
            //list.Add(new KeyValuePair<string, int>("Dog", 2));
            //list.Add(new KeyValuePair<string, int>("Rabbit", 4));

            List<KeyValuePair<string, string>> listVendorDatasets = null;

            getVendorDatasets(out listVendorDatasets);

            string vendor = "";
            string dataset = "";
            int FilesTotal = 0;
            int FilesDownloaded = 0;


            foreach (KeyValuePair<string, string> element in listVendorDatasets)
            {

                vendor = element.Key.ToString();
                dataset = element.Value.ToString();
                bool downloaded = IsVendorDatasetDownloaded(vendor, dataset, sProcessDate, out FilesTotal, out FilesDownloaded);
                logHelper.WriteLine("Vendor | " + vendor + " | Dataset | " + dataset + " | sProcessDate | " + sProcessDate + " | FilesDownloaded | "
                                    + FilesDownloaded + " | FilesTotal | " + FilesTotal);
            }

        }

        private bool IsVendorDatasetDownloaded( string Vendor, string Dataset, string sProcessDate, out int FilesTotal, out int FilesDownloaded)
        {
            FilesTotal = 0;
            FilesDownloaded = 0;
            bool isDownloaded = false;
            SqlCommand cmd = null;

            string commandText = @"
                select count(*) as FilesTotal from VIFs
                where Vendor = @Vendor and DataSet = @Dataset and [Application] = 'IDX' and Active = 'Yes'
                ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlAmdVifs,
                    CommandText = commandText
                };

                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters["@Vendor"].Value = Vendor;
                cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                cmd.Parameters["@Dataset"].Value = Dataset;

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
                    //string val = dr[0].ToString();
                    string val = dr["FilesTotal"].ToString();
                    FilesTotal = Convert.ToInt32(val);
                }
                dr.Close();

                if( FilesTotal > 0)
                {
                    cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                    cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                    cmd.CommandText = @"
                        select count(*) as FilesDownloaded from VIFs
                        where Vendor = @Vendor and DataSet = @Dataset and LastProcessDate = @ProcessDate 
                        and [Application] = 'IDX' and Active = 'Yes'
                        ";
                    dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        dr.Read();
                        string val = dr[0].ToString();
                        val = dr["FilesDownloaded"].ToString();
                        FilesDownloaded = Convert.ToInt32(val);
                        isDownloaded = (FilesTotal.Equals(FilesDownloaded) == true);
                    }
                    dr.Close();
                }
            }
            catch (SqlException ex)
            {
                //LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                //  LogHelper.WriteLine(logFuncName + " done " );
            }

            return (isDownloaded);
        }

        private void getVendorDatasets(out List<KeyValuePair<string,string>> listVendorDatasets)
        {
            string logFuncName = "getVendorDatasets: ";

            //LogHelper.WriteLine(logFuncName );

            listVendorDatasets = new List<KeyValuePair<string, string>>();

            SqlCommand cmd = null;
            string vendor = "";
            string dataset = "";

            string commandText = @"
                SELECT distinct Vendor, Dataset
                FROM Jobs
                where JobType = 'Vendor' and Active = 'Yes'
                order by Vendor, DataSet 
                ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = commandText
                };

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                int rows = 0;
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        rows += 1;
                        vendor = dr["Vendor"].ToString();
                        dataset = dr["Dataset"].ToString();
                        listVendorDatasets.Add(new KeyValuePair<string, string>(vendor, dataset));
                    }
                }
                dr.Close();
            }
            catch (SqlException ex)
            {
                //LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                //  LogHelper.WriteLine(logFuncName + " done " );
            }
            return;
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
