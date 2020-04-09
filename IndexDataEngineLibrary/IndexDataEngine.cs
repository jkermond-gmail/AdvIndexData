using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Configuration;
using System.IO;
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
            testing = AppSettings.Get<bool>("testingIndexDataEngine");
        }

        private void InitializeConnectionStrings()
        {
            sConnectionIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            sConnectionAmdVifs = ConfigurationManager.ConnectionStrings["dbConnectionAmdVifs"].ConnectionString;
        }

        public void TestGenerateStatusReport(string sProcessDate)
        {
            DateTime date = DateTime.Parse(sProcessDate);
            sProcessDate = date.ToString("MM/dd/yyyy");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();
            IndexDataProcessDate = date;
            GenerateStatusReport(sProcessDate);
            EndSql();
        }

        public void ProcessSecurityMasterChanges(string sProcessDate)
        {
            DateTime date = DateTime.Parse(sProcessDate);
            sProcessDate = date.ToString("MM/dd/yyyy");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();
            IndexDataProcessDate = date;
            GenerateSecurityMasterChangesData(sProcessDate);
            EndSql();
        }

        public void ProcessSecurityMasterReport(string sProcessDate)
        {
            DateTime date = DateTime.Parse(sProcessDate);
            sProcessDate = date.ToString("MM/dd/yyyy");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();
            IndexDataProcessDate = date;
            GenerateSecurityMasterChangesReport(sProcessDate);
            CopySecurityMasterChangesToFtpFolders(sProcessDate);
            EndSql();
        }

        public void GenerateSecurityMasterChangesDataAndReport(string sProcessDate)
        {
            DateTime date = DateTime.Parse(sProcessDate);
            sProcessDate = date.ToString("MM/dd/yyyy");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();
            IndexDataProcessDate = date;
            GenerateSecurityMasterChangesData(sProcessDate);
            GenerateSecurityMasterChangesReport(sProcessDate);
            CopySecurityMasterChangesToFtpFolders(sProcessDate);
            EndSql();
        }


        public void Run(string sVifsProcessDate)
        {
            DateTime date = DateTime.Parse(sVifsProcessDate);
            sVifsProcessDate = date.ToString("MM/dd/yyyy");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();
            VifsProcessDate = DateTime.ParseExact(sVifsProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            InitializeProcessStatus(sVifsProcessDate);
            testing = true;
            ProcessIndexDataDatasets(sVifsProcessDate, false);
            //ProcessIndexDataDatasets(sVifsProcessDate, false); // A second call will make sure the sp 900, 1000, and 1500 are complete
            EndSql();
        }


        public void Run()
        {
            //LogHelper.Info("IndexDataEngine.Run", "IndexDataEngineLibrary");
            InitializeConnectionStrings();
            DateHelper.ConnectionString = sConnectionAmdVifs;
            ProcessStatus.ConnectionString = sConnectionIndexData;
            BeginSql();

            sVifsProcessDate = getVIFsProcessDate();
            VifsProcessDate = DateTime.ParseExact(sVifsProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            sIndexDataProcessDate = getIndexDataProcessDate();

            IndexDataProcessDate = DateTime.ParseExact(sIndexDataProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);

            GenerateStatusReportIfNeeded(sVifsProcessDate, 21); //9PM
            GenerateStatusReportIfNeeded(sVifsProcessDate, 22); //10PM
            GenerateStatusReportIfNeeded(sVifsProcessDate, 23); //11PM


            if(VifsProcessDate.Date > IndexDataProcessDate.Date)
            {
                // Initialize everything cuz its a new day
                LogHelper.ArchiveLog(IndexDataProcessDate.Date);
                InitializeProcessStatus(sVifsProcessDate);
                setIndexDataProcessDate(sVifsProcessDate);
                DeleteFilesInFtpFolders();
                InitializeHistoricalSecurityMasterCopy();
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: New business day started " + sVifsProcessDate);
            }

            ProcessIndexDataDatasets(sVifsProcessDate, true);

            if (testing)
            {
                ProcessIndexDataDatasets(sVifsProcessDate, true); // A second call will make sure the sp 900, 1000, and 1500 are complete
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
            ProcessStatus.DeleteOldEntries(sProcessDate);
            ProcessStatus.DeleteEntries(sProcessDate);

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

        private void ProcessIndexDataDatasets(string sProcessDate, bool normalProcessing)
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

            foreach (KeyValuePair<string, string> element in listVendorDatasets)
            {
                vendor = element.Key.ToString();
                dataset = element.Value.ToString();

                if (normalProcessing.Equals(true))
                {
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
                else
                {
                    ProcessVendorDatasetJobs(vendor, dataset, sProcessDate, out JobsTotal, out JobsProcessed);
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

        public bool VendorFilesDownloaded(string sProcessDate, out int FilesTotal, out int FilesDownloaded)
        {
            FilesTotal = 0;
            FilesDownloaded = 0;
            bool isDownloaded = false;
            SqlCommand cmd = null;
            string logFuncName = "VendorFilesDownloaded: ";

            string commandText = @"
                select count(*) as FilesTotal from VIFs
                where [Application] = 'IDX' and Active = 'Yes'
                ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlAmdVifs,
                    CommandText = commandText
                };

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
                    string val = dr["FilesTotal"].ToString();
                    FilesTotal = Convert.ToInt32(val);
                }
                dr.Close();

                if (FilesTotal > 0)
                {
                    cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                    cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                    cmd.CommandText = @"
                        select count(*) as FilesDownloaded from VIFs
                        where LastProcessDate = @ProcessDate 
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


        public List<string> VendorFilesNotDownloaded(string sProcessDate)
        {
            SqlCommand cmd = null;
            var VendorFiles = new List<string>();
            string logFuncName = "VendorFilesNotDownloaded: ";


            string commandText = @"
                select * from VIFs
                where LastProcessDate < @ProcessDate 
                and [Application] = 'IDX' and Active = 'Yes'
                ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlAmdVifs,
                    CommandText = commandText
                };

                cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        VendorFiles.Add(dr["InternetFilename"].ToString());
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

            return (VendorFiles);
        }

        private void GenerateStatusReport(string sProcessDate)
        {
            int FilesTotal = 0;
            int FilesDownloaded = 0;
            var report = new List<string>();

            report.Add("Advent Index Data status report for " + sProcessDate);
            report.Add("------------------------------------------------");

            bool bFilesDownloaded = VendorFilesDownloaded(sProcessDate, out FilesTotal, out FilesDownloaded);

            if (!bFilesDownloaded)
            {
                report.Add("Missing Vendor Files not downloaded:");

                List<string> files = VendorFilesNotDownloaded(sProcessDate);
                string sYYYYMMDD = DateHelper.ConvertToYYYYMMDD(sProcessDate);
                foreach (string file in files)
                {
                    string file2 = file.Replace("YYYYMMDD", sYYYYMMDD);
                    report.Add("   " + file2);
                }
            }
            else
            {
                report.Add(FilesDownloaded + " of " + FilesTotal + " Vendor Files downloaded");
            }

            int TotalProcessStatusRows = 0;
            int CountAxmlConstituentData = 0;
            int CountAxmlSectorData = 0;
            int ExpectedConstituentClientFiles = 0;
            int ActualConstituentClientFiles = 0;
            int ExpectedSectorClientFiles = 0;
            int ActualSectorClientFiles = 0;

            ProcessStatus.GetStatusSummary(sProcessDate, out TotalProcessStatusRows, out CountAxmlConstituentData, out CountAxmlSectorData,
                                            out ExpectedConstituentClientFiles, out ActualConstituentClientFiles, out ExpectedSectorClientFiles, out ActualSectorClientFiles);

            report.Add(CountAxmlConstituentData + " out of  " + TotalProcessStatusRows + " System Job AXML Constituent files successfully generated");
            report.Add(CountAxmlSectorData + " out of  " + TotalProcessStatusRows + " System Job AXML Sector files successfully generated");
            report.Add(ActualConstituentClientFiles + " out of  " + ExpectedConstituentClientFiles + " Client Constituent files successfully copied to ftp folders");
            report.Add(ActualSectorClientFiles + " out of  " + ExpectedSectorClientFiles + " Client Sector files successfully copied to ftp folders");

            string mailMsg = "";
            foreach (string reportRow in report)
            {
                LogHelper.WriteLine(reportRow);
                mailMsg += reportRow + "\r\n";
            }

            Mail mail = new Mail();
            mail.SendMail(mailMsg);
        }


        private void GenerateStatusReportIfNeeded(string sProcessDate, int Hour)
        {
            string sReportDate = getSystemSettingValue("StatusReportDate" + Hour.ToString(), cnSqlIndexData); 
            DateTime reportDate = DateTime.ParseExact(sReportDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
            DateTime now = DateTime.Now;

            if(reportDate < IndexDataProcessDate)
            {                
                if(now.Hour.Equals(Hour))
                {
                    TimeSpan timeOfDay = DateTime.Now.TimeOfDay;
                    TimeSpan start = new TimeSpan(Hour, 0, 0);    // Hour:00PM
                    TimeSpan end = new TimeSpan(Hour, 5, 0);      // Hour:05PM

                    if((timeOfDay >= start) && (timeOfDay <= end))
                    {
                        GenerateStatusReport(sProcessDate);
                        setSystemSettingValue("StatusReportDate" + Hour.ToString(), sProcessDate, cnSqlIndexData);
                    }
                }
            }
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

        private string getSystemSettingValue(string SettingName, SqlConnection sqlConnection)
        {
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

        public void CreateFtpFolders()
        {
            InitializeConnectionStrings();
            BeginSql();
            string ftpRootDir = AppSettings.Get<string>("ftpRootDir");
            string dir = "";

            if (!Directory.Exists(ftpRootDir))
            {
                Directory.CreateDirectory(ftpRootDir);
            }

            SqlCommand cmd = null;
            string clientID = "";


            string commandText = @"
                SELECT distinct ClientID
                FROM Clients
                ORDER BY ClientID
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
                        clientID = dr["ClientID"].ToString();
                        dir = ftpRootDir + "\\" + clientID;
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }

                        dir = dir + "\\" + "IndexData";
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }

                        dir = dir + "\\" + "Results";
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }
                    }
                }
                dr.Close();
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message );
            }
            finally
            {
                //LogHelper.WriteLine(logFuncName + " done " );
                EndSql();
            }
            return;

        }

        public void DeleteFilesInFtpFolders()
        {
            string ftpRootDir = AppSettings.Get<string>("ftpRootDir");
            string dir = "";
            string filename = "";
            SqlCommand cmd = null;
            string clientID = "";

            bool deleteFilesInFtpFolders = AppSettings.Get<bool>("deleteFilesInFtpFolders");

            if (deleteFilesInFtpFolders)
            {
                string commandText = @"
                SELECT distinct ClientID
                FROM Clients
                ORDER BY ClientID
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
                            clientID = dr["ClientID"].ToString();
                            dir = ftpRootDir + "\\" + clientID;
                            if (Directory.Exists(dir))
                            {
                                dir = dir + "\\" + "IndexData";
                                if (Directory.Exists(dir))
                                {
                                    dir = dir + "\\" + "Results";
                                    if (Directory.Exists(dir))
                                    {
                                        DirectoryInfo di = new DirectoryInfo(dir);

                                        foreach (FileInfo file in di.GetFiles())
                                        {
                                            filename = file.FullName;
                                            file.Delete();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    dr.Close();
                }
                catch (SqlException ex)
                {
                    LogHelper.WriteLine(ex.Message);
                    LogHelper.WriteLine("Unsuccessful delete " + filename);
                }
                finally
                {
                    //LogHelper.WriteLine(logFuncName + " done " );
                }
            }
            return;
        }

        public void InitializeHistoricalSecurityMasterCopy()
        {
            bool endSql = false;
            try
            {
                LogHelper.WriteLine("InitializeHistoricalSecurityMasterCopy");
                if (String.IsNullOrEmpty(sConnectionIndexData))
                {
                    LogHelper.WriteLine("sConnectionIndexData is null");

                    InitializeConnectionStrings();
                    BeginSql();
                    endSql = true;
                }

                string sqlDelete = "DELETE FROM HistoricalSecurityMasterFullCopy";

                SqlCommand cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = sqlDelete
                };
                cmd.ExecuteNonQuery();
                LogHelper.WriteLine("Delete is done");

                cmd.CommandText = @"
                    INSERT INTO HistoricalSecurityMasterFullCopy (
	                id, Ticker, Cusip, Vendor, StockKey, CompanyName, SectorCode, Exchange, BeginDate, EndDate)
	                SELECT id, Ticker, Cusip, Vendor, StockKey, CompanyName, SectorCode, Exchange, BeginDate, EndDate
	                FROM HistoricalSecurityMasterFull ORDER BY id
                ";
                cmd.ExecuteNonQuery();
                LogHelper.WriteLine("Insert is done");

            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine("InitializeHistoricalSecurityMasterCopy Sql Exception " + ex.Message);

                if (ex.Number == 2627)
                {
                    LogHelper.WriteLine(ex.Message);
                }
            }
            finally
            {
                if (endSql)
                    EndSql();
            }
            
        return;

        }

        public void GenerateSecurityMasterChangesData(string sProcessDate)
        {
            LogHelper.WriteLine("GenerateSecurityMasterChangesData " + sProcessDate);
            SqlCommand cmd = null;
            SqlCommand cmd2 = null;
            SqlConnection cnSql2 = null ;
            // First Get the new adds
            string selectText = @"
                select id from HistoricalSecurityMasterFull where id not in 
                (select id from HistoricalSecurityMasterFullCopy)
            ";
            string insertText = @"
                insert into HistoricalSecurityMasterFullChanges
                (id, ProcessDate, ChangeType, CusipNew, TickerNew, CompanyNameNew, SectorCodeNew, ExchangeNew)
                select id, BeginDate, 'Add', Cusip, Ticker, CompanyName, SectorCode, Exchange from HistoricalSecurityMasterFull where id = @id
            ";
            try
            {
                cnSql2 = new SqlConnection(sConnectionIndexData);
                cnSql2.Open();

                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = selectText
                };

                cmd2 = new SqlCommand
                {
                    Connection = cnSql2,
                    CommandText = insertText
                };
                cmd2.Parameters.Add("@id", SqlDbType.Int);

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        string val = dr["id"].ToString();
                        int id = Convert.ToInt32(val);
                        cmd2.Parameters["@id"].Value = id;
                        cmd2.ExecuteNonQuery();
                        LogHelper.WriteLine("Insert Add is done");
                    }
                }
                dr.Close();

                // Next get deletes
                string sPrevProcessDate = DateHelper.PrevBusinessDayMMDDYYYY_Slash(sProcessDate);
                selectText = @"
                    select id from HistoricalSecurityMasterFullCopy where EndDate = @PrevProcessDate and id in
                    (select id from HistoricalSecurityMasterFull where EndDate = @PrevProcessDate)
                ";
                cmd.CommandText = selectText;
                cmd.Parameters.Add("@PrevProcessDate", SqlDbType.Date);
                cmd.Parameters["@PrevProcessDate"].Value = sPrevProcessDate;

                insertText = @"
                    insert into HistoricalSecurityMasterFullChanges
                    (id, ProcessDate, ChangeType, Cusip, Ticker, CompanyName, SectorCode, Exchange)
                    select id, @ProcessDate, 'Delete', Cusip, Ticker, CompanyName, SectorCode, Exchange from HistoricalSecurityMasterFull where id = @id
                ";
                cmd2.CommandText = insertText;
                cmd2.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd2.Parameters["@ProcessDate"].Value = sProcessDate;


                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        string val = dr["id"].ToString();
                        int id = Convert.ToInt32(val);
                        cmd2.Parameters["@id"].Value = id;
                        cmd2.ExecuteNonQuery();
                        LogHelper.WriteLine("Insert Delete are done");
                    }
                }
                dr.Close();

                // Lastly, get updates
                selectText = @"
                    select h.id from HistoricalSecurityMasterFull h
                    inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
                    where h.EndDate = @ProcessDate and hprev.EndDate = @PrevProcessDate
                    and h.Vendor = 'R' and hprev.Vendor = 'R' 
                    and (h.Ticker <> hprev.Ticker OR h.Cusip <> hprev.Cusip or h.CompanyName <> hprev.CompanyName or h.SectorCode <> hprev.SectorCode
                    or h.Exchange <> hprev.Exchange)
                ";
                cmd.CommandText = selectText;
                cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;

                insertText = @"
                    insert into HistoricalSecurityMasterFullChanges
                    (id, ProcessDate, ChangeType, Cusip, CusipNew, Ticker, TickerNew, CompanyName, CompanyNameNew, SectorCode, SectorCodeNew, Exchange, ExchangeNew)
                    select h.id, h.EndDate, 'Update', hprev.Cusip, h.Cusip, hprev.Ticker, h.Ticker, hprev.CompanyName, h.CompanyName, 
                    hprev.SectorCode, h.SectorCode, hprev.Exchange, h.Exchange
	                from HistoricalSecurityMasterFull h
                    inner join HistoricalSecurityMasterFullCopy hprev on h.id = hprev.id
                    where h.id = @id and hprev.id = @id
                ";
                cmd2.CommandText = insertText;

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        string val = dr["id"].ToString();
                        int id = Convert.ToInt32(val);
                        cmd2.Parameters["@id"].Value = id;
                        cmd2.ExecuteNonQuery();
                        LogHelper.WriteLine("Insert Updates are done");
                    }
                }
                dr.Close();
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }
            finally
            {
                if (cnSql2 != null)
                    cnSql2.Close();
                LogHelper.WriteLine("GenerateSecurityMasterChangesData Done");
            }

            return;
        }


        /*
         * old report from old production version
        Date     Action Cusip     New Cusip  Ticker     New Ticker Exchange     New Exchange 1 2 5 M T S 3 MICRO 1 2 5 M T S 3 MICRO Name                      New Name                  ES     New ES SubS     New SubS IND      New IND
        -------- ------ --------- ---------- ---------- ---------- ------------ ------------ - - - - - - - ----- - - - - - - - ----- ------------------------- ------------------------- ------ ------ -------- -------- -------- --------
        20190520 Delete 45685L100            HIFR                  NYSE                      N Y Y N N Y Y N                         INFRAREIT INC                                       10            1060              1060914           
        20190520 Delete 577767106            MXWL                  NASDAQ                    N Y Y N N Y Y Y                         MAXWELL TECHNOLOGIES INC                            08            0820              0820734           
        20190520 Delete 58409L306            MRT                   NYSE                      N Y Y N N Y Y Y                         MEDEQUITIES REALTY TRUST                            10            1060              1060910           
        20190520 Update 90539J109  04911A107 UBSH       AUB        NASDAQ                    N Y Y N N Y Y N                         UNION BANKSHARES CORP      ATLANTIC UNION BANKSHARE 10            1010              1010400           
        20190520 Update 91914N202  91914N301 VLRX                  NASDAQ                    N N N N N N N Y                         VALERITAS HOLDINGS INC                              02            0220              0220471           

            new report from new production version Mar 2020

        Date     Action Cusip     New Cusip  Ticker     New Ticker Name                      New Name                  Sector  New Sector Exchange     New Exchange 
        -------- ------ --------- ---------- ---------- ---------- ------------------------- ------------------------- ------- ---------- ------------ ------------"
        20190904 Add              01234567              ATest                                Atest Co                          1234567    NYSE         NASDAQ
        20190904 Update 12345678  1234567x   DUMMY      DUMMY      NA                        NA                        1234567 1234567 
        20190904 Update 52603A20  52603A20   LC         LC         LENDINGCLUB CORPORATION   LENDINGCLUB CORPORATIONx  1020489 1020489 
        20190904 Update 69320M10  69320M10   PCB        PCB        PCB BANCORP               PCB BANCORP               1010400 101040x 

         */

        public void GenerateSecurityMasterChangesReport(string sProcessDate)
        {
            LogHelper.WriteLine("GenerateSecurityMasterChangesReport " + sProcessDate);
            string sAxmlOutputPath = AppSettings.Get<string>("AxmlOutputPath");
            string filename = sAxmlOutputPath + "rl_" + DateHelper.ConvertToYYYYMMDD(sProcessDate) + "_RefData.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using (StreamWriter file = new StreamWriter(filename))
            {
                file.WriteLine("Date     Action Cusip     New Cusip  Ticker     New Ticker Name                      New Name                  Sector  New Sector Exchange     New Exchange ");
                file.WriteLine("-------- ------ --------- ---------- ---------- ---------- ------------------------- ------------------------- ------- ---------- ------------ ------------");
            }

            SqlCommand cmd = null;
            string selectText = @"
                select * from HistoricalSecurityMasterFullChanges where ProcessDate = @ProcessDate
                order by ChangeType, Cusip
            ";
            try
            {
                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = selectText
                };
                cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                using (StreamWriter file = new StreamWriter(filename, true))
                {
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            string ProcessDate = GetColString(dr, "ProcessDate");
                            if (ProcessDate.Length > 0)
                                ProcessDate = DateHelper.ConvertToYYYYMMDD(ProcessDate);
                            ProcessDate = ProcessDate.PadRight(8 + 1);
                            string ChangeType = GetColString(dr, "ChangeType");
                            ChangeType = ChangeType.PadRight(6 + 1);
                            string Cusip = GetColString(dr, "Cusip");
                            Cusip = Cusip.PadRight(9 + 1);
                            string CusipNew = GetColString(dr, "CusipNew");
                            CusipNew = CusipNew.PadRight(9 + 2);
                            string Ticker = GetColString(dr, "Ticker");
                            Ticker = Ticker.PadRight(10 + 1);
                            string TickerNew = GetColString(dr, "TickerNew");
                            TickerNew = TickerNew.PadRight(10 + 1);
                            string CompanyName = GetColString(dr, "CompanyName");
                            CompanyName = CompanyName.PadRight(25 + 1);
                            string CompanyNameNew = GetColString(dr, "CompanyNameNew");
                            CompanyNameNew = CompanyNameNew.PadRight(25 + 1);
                            string SectorCode = GetColString(dr, "SectorCode");
                            SectorCode = SectorCode.PadRight(7 + 1);
                            string SectorCodeNew = GetColString(dr, "SectorCodeNew");
                            SectorCodeNew = SectorCodeNew.PadRight(7 + 4);
                            string Exchange = GetColString(dr, "Exchange");
                            Exchange = Exchange.PadRight(12 + 1);
                            string ExchangeNew = GetColString(dr, "ExchangeNew");
                            ExchangeNew = ExchangeNew.PadRight(12 + 1);

                            file.WriteLine(ProcessDate + ChangeType + Cusip + CusipNew + Ticker + TickerNew + CompanyName + CompanyNameNew + SectorCode + SectorCodeNew
                                           + Exchange + ExchangeNew);
                        }
                    }
                    else
                    {
                        file.WriteLine(DateHelper.ConvertToYYYYMMDD(sProcessDate) + " No Changes");
                    }
                }
                dr.Close();
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }
            finally
            {
                LogHelper.WriteLine("GenerateSecurityMasterChangesReport done");
            }

            return;
        }

        public static string GetColString(SqlDataReader dr, string colName)
        {
            string colString = "";
            if (!dr.IsDBNull(dr.GetOrdinal(colName)))
            {
                colString = dr[colName].ToString();
            }
            return (colString);
        }

        public void CopySecurityMasterChangesToFtpFolders(string sFileDate)
        {
            SqlCommand cmd = null;
            SqlDataReader dr = null;
            try
            {
                string SqlSelect = @"
                    select distinct ClientID
                    from Jobs
                    where RefReport = 'txt,html' and Active = 'Yes' and ClientID <> 'SystemClient'
                    order by ClientID
                ";

                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = SqlSelect
                };

                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    string ftpRootDir = AppSettings.Get<string>("ftpRootDir");
                    string sourceDir = AppSettings.Get<string>("AxmlOutputPath");
                    string clientID = null;
                    string destFilename = null;

                    while (dr.Read())
                    {
                        clientID = dr["ClientID"].ToString();

                        string destDir = ftpRootDir + "\\" + clientID + "\\IndexData\\Results\\";

                        if (Directory.Exists(sourceDir) && Directory.Exists(destDir))
                        {
                            string sourceFilename = "";

                            sourceFilename = sourceDir + "rl_" + DateHelper.ConvertToYYYYMMDD(sFileDate) + "_RefData.txt";

                            if (File.Exists(sourceFilename))
                            {
                                destFilename = destDir + Path.GetFileName(sourceFilename);
                                File.Copy(sourceFilename, destFilename, true);
                                LogHelper.WriteLine("Successful copy " + destFilename);
                            }
                            else
                            {
                                LogHelper.WriteLine("Missing sourcefile" + sourceFilename);
                            }
                        }
                    }
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
            }
        }
    }
}
