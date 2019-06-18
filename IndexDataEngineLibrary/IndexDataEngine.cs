﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Configuration;
using AdventUtilityLibrary;


namespace IndexDataEngineLibrary
{
    public class IndexDataEngine
    {
        private string sConnectionIndexData = null;
        private string sConnectionAmdVifs = null;

        private SqlConnection cnSqlIndexData = null;
        private SqlConnection cnSqlAmdVifs = null;
        private string sVifsProcessDate;
        private DateTime VifsProcessDate;
        private string sIndexDataProcessDate;
        private DateTime IndexDataProcessDate;
        private bool testing = false;

        public IndexDataEngine()
        {
            //LogHelper.Info("IndexDataEngine()", "IndexDataEngineLibrary");
            testing = AppSettings.Get<bool>("testingIndexDataEngine");
            //LogHelper.WriteLine("testingIndexDataEngine = " + testing);
        }


        public void Run()
        {
            //LogHelper.Info("IndexDataEngine.Run", "IndexDataEngineLibrary");
            sConnectionIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            sConnectionAmdVifs = ConfigurationManager.ConnectionStrings["dbConnectionAmdVifs"].ConnectionString;
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;


            BeginSql();

            sVifsProcessDate = getVIFsProcessDate();
            VifsProcessDate = DateTime.ParseExact(sVifsProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            sIndexDataProcessDate = getIndexDataProcessDate();
            IndexDataProcessDate = DateTime.ParseExact(sIndexDataProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);

            if (VifsProcessDate.Date > IndexDataProcessDate.Date)
            {
                // Initialize everything cuz its a new day
                LogHelper.ArchiveLog(IndexDataProcessDate.Date);
                InitializeProcessStatus(sVifsProcessDate);
                setIndexDataProcessDate(sVifsProcessDate);
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: New business day started " + sVifsProcessDate);
            }

            ProcessIndexDataWork(sVifsProcessDate);

            if (testing)
            {                
                ProcessIndexDataWork(sVifsProcessDate); // A second call will make sure the sp 900, 1000, and 1500 are complete
                string sToday = DateTime.Now.ToString("MM/dd/yyyy");
                if (sVifsProcessDate.Equals(sToday)) // JK to do change
                {
                    testing = false;
                }
                else
                {
                VifsProcessDate = DateHelper.NextBusinessDay(VifsProcessDate);
                sVifsProcessDate = VifsProcessDate.ToString("MM/dd/yyyy");
                setVIFsProcessDate(sVifsProcessDate);
                }
            }
            EndSql();
        }

        private void InitializeProcessStatus(string sProcessDate)
        {
            string Vendor = "";
            string Dataset = "";
            string IndexName = "";
            LogHelper.WriteLine("InitializeProcessStatus: " + sProcessDate.ToString());

            ProcessStatus.Initialize();
            List<KeyValuePair<string, string>> listVendorDatasets = null;

            getVendorDatasets(out listVendorDatasets);

            foreach (KeyValuePair<string, string> element in listVendorDatasets)
            {
                Vendor = element.Key.ToString();
                Dataset = element.Value.ToString();
                if (Vendor.Equals("Russell"))
                {
                    RussellData russellData = new RussellData();
                    string[] Indices = null;
                    Indices = russellData.GetIndices();
                    for (int i = 0; i < Indices.Length; i++)
                    {
                        IndexName = Indices[i];
                        ProcessStatus.Add(sProcessDate, Vendors.Russell.ToString(), Dataset, IndexName);
                    }
                }
                else if (Vendor.Equals("StandardAndPoors"))
                {
                    IndexName = Dataset;
                    ProcessStatus.Add(sProcessDate, Vendors.Snp.ToString(), Dataset, IndexName);
                }
            }
        }

        private void ProcessIndexDataWork(string sProcessDate)
        {
            //LogHelper.WriteLine("ProcessIndexDataWork: " + sProcessDate.ToString());

            List<KeyValuePair<string, string>> listVendorDatasets = null;

            getVendorDatasets(out listVendorDatasets);

            string vendor = "";
            string dataset = "";
            int FilesTotal = 0;
            int FilesDownloaded = 0;
            int JobsTotal = 0;
            int JobsProcessed = 0;
            //int FilesGenerated = 0;


            foreach (KeyValuePair<string, string> element in listVendorDatasets)
            {
                vendor = element.Key.ToString();
                dataset = element.Value.ToString();

                if (testing)
                    VendorDatasetFilesUpdateLastProcessDate(vendor, dataset, sProcessDate);

                if (VendorDatasetFilesDownloaded(vendor, dataset, sProcessDate, out FilesTotal, out FilesDownloaded))
                {
                    if (FilesDownloaded < FilesTotal)
                        LogHelper.WriteLine("Vendor | " + vendor + " | Dataset | " + dataset + " | sProcessDate | " + sProcessDate + " | FilesDownloaded | "
                                            + FilesDownloaded + " | FilesTotal | " + FilesTotal);

                    if (VendorDatasetJobsProcessed(vendor, dataset, sProcessDate, out JobsTotal, out JobsProcessed))
                    {
                        if (JobsProcessed < JobsTotal)
                            LogHelper.WriteLine("Vendor | " + vendor + " | Dataset | " + dataset + " | sProcessDate | " + sProcessDate + " | JobsProcessed | "
                                            + JobsProcessed + " | JobsTotal | " + JobsTotal);
                    }
                    else
                    {
                        ProcessVendorDatasetJobs(vendor, dataset, sProcessDate, out JobsTotal, out JobsProcessed);
                    }
                }

            }

        }

        private bool VendorDatasetFilesGenerated(string Vendor, string Dataset, string sProcessDate, out int FilesTotal, out int FilesGenerated)
        {
            FilesTotal = 0;
            FilesGenerated = 0;
            bool isGenerated = false;

            return (isGenerated);
        }


        private bool VendorDatasetFilesDownloaded( string Vendor, string Dataset, string sProcessDate, out int FilesTotal, out int FilesDownloaded)
        {
            FilesTotal = 0;
            FilesDownloaded = 0;
            bool isDownloaded = false;
            SqlCommand cmd = null;
            string Dataset1 = "";
            string Dataset2 = "";
            string Dataset3 = "";
            string logFuncName = "AreVendorDatasetFilesDownloaded: ";


            if( Dataset.Equals("sp900"))
            {
                Dataset1 = "sp400";
                Dataset2 = "sp500";
                Dataset2 = "sp500";
            }
            else if( Dataset.Equals("sp1000"))
            {
                Dataset1 = "sp400";
                Dataset2 = "sp600";
                Dataset2 = "sp600";
            }
            else if( Dataset.Equals("sp1500"))
            {
                Dataset1 = "sp400";
                Dataset2 = "sp500";
                Dataset2 = "sp600";
            }
            else
            {
                Dataset1 = Dataset;
                Dataset2 = Dataset;
                Dataset3 = Dataset;
            }

            string commandText = @"
                select count(*) as FilesTotal from VIFs
                where Vendor = @Vendor and (DataSet = @Dataset1 or DataSet = @Dataset2 or DataSet = @Dataset3)  
                and [Application] = 'IDX' and Active = 'Yes'
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
                cmd.Parameters.Add("@Dataset1", SqlDbType.VarChar);
                cmd.Parameters["@Dataset1"].Value = Dataset1;
                cmd.Parameters.Add("@Dataset2", SqlDbType.VarChar);
                cmd.Parameters["@Dataset2"].Value = Dataset2;
                cmd.Parameters.Add("@Dataset3", SqlDbType.VarChar);
                cmd.Parameters["@Dataset3"].Value = Dataset3;

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
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
                        where Vendor = @Vendor and (DataSet = @Dataset1 or DataSet = @Dataset2 or DataSet = @Dataset3) 
                        and LastProcessDate = @ProcessDate 
                        and [Application] = 'IDX' and Active = 'Yes'
                        ";
                    dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        dr.Read();
                        string val = dr["FilesDownloaded"].ToString();
                        FilesDownloaded = Convert.ToInt32(val);
                        isDownloaded = (FilesTotal.Equals(FilesDownloaded) == true);
                    }
                    dr.Close();
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                //LogHelper.WriteLine(logFuncName + " done " );
            }

            return (isDownloaded);
        }

        private void VendorDatasetFilesUpdateLastProcessDate(string Vendor, string Dataset, string sProcessDate)
        {
            SqlCommand cmd = null;
            string logFuncName = "VendorDatasetFilesUpdateLastProcessDate: ";


            string commandText = @"
                Update VIFs set LastProcessDate = @LastProcessDate
                where Vendor = @Vendor and DataSet = @Dataset and [Application] = 'IDX' and Active = 'Yes'
                ";

            List<string> dataSets = new List<string>();

            if( Dataset.Equals("sp900"))
            {
                dataSets.Add("sp400");
                dataSets.Add("sp500");
            }
            else if( Dataset.Equals("sp1000"))
            {
                dataSets.Add("sp400");
                dataSets.Add("sp600");
            }
            else if (Dataset.Equals("sp1500"))
            {
                dataSets.Add("sp400");
                dataSets.Add("sp500");
                dataSets.Add("sp600");
            }
            else
            {
                dataSets.Add(Dataset);
            }


            foreach (string dataSet in dataSets)
            {
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
                    cmd.Parameters["@Dataset"].Value = dataSet;
                    cmd.Parameters.Add("@LastProcessDate", SqlDbType.Date);
                    cmd.Parameters["@LastProcessDate"].Value = sProcessDate;
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

        private void ProcessVendorDatasetJobs(string Vendor, string Dataset, string sProcessDate, out int JobsTotal, out int JobsProcessed)
        {
            JobsTotal = 0;
            JobsProcessed = 0;

            if (Vendor.Equals("Russell"))
            {
                RussellData russellData = new RussellData();
                russellData.ProcessVendorDatasetJobs(Dataset, sProcessDate);
            }
            else if (Vendor.Equals("StandardAndPoors"))
            {

                SnpData snpData = new SnpData();
                snpData.ProcessVendorDatasetJobs(Dataset, sProcessDate);
            }
            VendorDatasetJobsProcessed(Vendor, Dataset, sProcessDate, out JobsTotal, out JobsProcessed);
        }


        private bool VendorDatasetJobsProcessed(string Vendor, string Dataset, string sProcessDate, out int JobsTotal, out int JobsProcessed)
        {
            JobsTotal = 0;
            JobsProcessed = 0;
            bool Processed = false;
            SqlCommand cmd = null;
            string logFuncName = "VendorDatasetJobsProcessed: ";


            string commandText = @"
                SELECT count(*) as JobsTotal
                FROM Jobs
                WHERE  Vendor = @Vendor and DataSet = @Dataset and JobType = 'Vendor' and Active = 'Yes'
                ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
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
                    string val = dr["JobsTotal"].ToString();
                    JobsTotal = Convert.ToInt32(val);
                }
                dr.Close();

                if (JobsTotal > 0)
                {
                    cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                    cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                    cmd.CommandText = @"
                        SELECT count(*) as JobsProcessed
                        FROM Jobs
                        WHERE  Vendor = @Vendor and DataSet = @Dataset and LastProcessDate = @ProcessDate 
                        and JobType = 'Vendor' and Active = 'Yes'
                        ";
                    dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        dr.Read();
                        string val = dr["JobsProcessed"].ToString();
                        JobsProcessed = Convert.ToInt32(val);
                        Processed = (JobsTotal.Equals(JobsProcessed) == true);
                    }
                    dr.Close();
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                //LogHelper.WriteLine(logFuncName + " done ");
            }

            return (Processed);
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
                LogHelper.WriteLine(logFuncName + " " + ex.Message);
            }
            finally
            {
                //LogHelper.WriteLine(logFuncName + " done " );
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
                    LogHelper.WriteLine(ex.Message);
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
                LogHelper.WriteLine(ex.Message);
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

        private void setVIFsProcessDate(string sDate)
        {
            setSystemSettingValue("VIFLastProcessDate", sDate, cnSqlAmdVifs);
        }

        private void EndSql()
        {
            cnSqlIndexData.Close();
            cnSqlAmdVifs.Close();
        }


    }
}
