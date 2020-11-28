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

        //public void ProcessSecurityMasterChanges(string sProcessDate)
        //{
        //    DateTime date = DateTime.Parse(sProcessDate);
        //    sProcessDate = date.ToString("MM/dd/yyyy");
        //    InitializeConnectionStrings();
        //    DateHelper.ConnectionString = sConnectionAmdVifs;
        //    ProcessStatus.ConnectionString = sConnectionIndexData;
        //    BeginSql();
        //    IndexDataProcessDate = date;
        //    GenerateSecurityMasterChangesData(sProcessDate);
        //    EndSql();
        //}

        public void ProcessSecurityMasterReport(string sStartDate, string sEndDate)
        {
            DateTime startDate = Convert.ToDateTime(sStartDate);
            DateTime endDate = Convert.ToDateTime(sEndDate);
            DateTime processDate;
            int DateCompare;

            for(processDate = startDate
            ; (DateCompare = processDate.CompareTo(endDate)) <= 0
            ; processDate = DateHelper.NextBusinessDay(processDate))
            {
                ProcessSecurityMasterReport(processDate.ToString("MM/dd/yyyy"));
            }

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
            GenerateSecurityMasterChangesReport(sProcessDate, "4");
            CopySecurityMasterChangesToFtpFolders(sProcessDate);
            EndSql();
        }


        public void Run(string sStartDate, string sEndDate)
        {
            DateTime startDate = Convert.ToDateTime(sStartDate);
            DateTime endDate = Convert.ToDateTime(sEndDate);
            DateTime processDate;
            int DateCompare;

            for(processDate = startDate
            ; (DateCompare = processDate.CompareTo(endDate)) <= 0
            ; processDate = DateHelper.NextBusinessDay(processDate))
            {
                Run(processDate.ToString("MM/dd/yyyy"));
            }
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
                //InitializeHistoricalSecurityMasterCopy();
                Mail mail = new Mail();
                mail.SendMail("AdvIndexData: New business day started " + sVifsProcessDate);
            }

            ProcessIndexDataDatasets(sVifsProcessDate, true);

            if (testing)
            {
                //ProcessIndexDataDatasets(sVifsProcessDate, true); // A second call will make sure the sp 900, 1000, and 1500 are complete
                string sToday = DateTime.Now.ToString("MM/dd/yyyy");

                DateTime oProcessDate = DateTime.ParseExact(sVifsProcessDate, "MM/dd/yyyy", CultureInfo.InvariantCulture);
                DateTime oCurrentDate = DateTime.ParseExact(sToday, "MM/dd/yyyy", CultureInfo.InvariantCulture);

                if(oProcessDate >= oCurrentDate)
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
                else if(Vendor.Equals("RussellICB"))
                {
                    RussellIcbData russellIcbData = new RussellIcbData();
                    string[] Indices = null;
                    Indices = russellIcbData.GetIndices();
                    for(int i = 0; i < Indices.Length; i++)
                    {
                        IndexName = Indices[i];
                        ProcessStatus.Add(sProcessDate, Vendors.RussellIcb.ToString(), Dataset, IndexName);
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

            if (Vendor.Equals("RussellICB"))
            {
                RussellIcbData russellIcbData = new RussellIcbData();
                russellIcbData.ProcessVendorDatasetJobs(Dataset, sProcessDate);
            }
            else if(Vendor.Equals("Russell"))
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


        public void GenerateSecurityMasterChangesReport(string sProcessDate, string Vendor)
        {
            LogHelper.WriteLine("GenerateSecurityMasterChangesReport " + sProcessDate);
            string sAxmlOutputPath = AppSettings.Get<string>("AxmlOutputPath");
            string filename = sAxmlOutputPath + "4rl_" + DateHelper.ConvertToYYYYMMDD(sProcessDate) + "_RefData.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using (StreamWriter file = new StreamWriter(filename))
            {
                file.WriteLine("Date     Action Cusip      New Cusip   Ticker     New Ticker Name                      New Name                  Sector   New Sector Exchange     New Exchange ");
                file.WriteLine("-------- ------ ---------- ----------- ---------- ---------- ------------------------- ------------------------- -------- ---------- ------------ ------------");
            }

            SqlCommand cmd = null;

            try
            {
                /*
                Vendor ChangeDate              OldSymbol  NewSymbol  CompanyName
                ------ ----------------------- ---------- ---------- ------------------------------
                R      2020-05-07 00:00:00.000 91354310   90278Q10   UFP INDUSTRIES INC
                R      2020-05-04 00:00:00.000 45782F10   65440510   9 METERS BIOPHARMA INC
                R      2020-05-04 00:00:00.000 INNT       NMTR       9 METERS BIOPHARMA INC
                R      2020-04-28 00:00:00.000 DO         DOFSQ      DIAMOND OFFSHR DRILLING
                R      2020-04-24 00:00:00.000 FTR        FTRCQ      FRONTIER COMMUNICATIONS

                 */
                string selectText = @"
                SELECT * FROM
                  HistoricalSymbolChanges WHERE ChangeDate = @ProcessDate and Vendor = @Vendor
                ";
                cmd = new SqlCommand
                {
                    Connection = cnSqlIndexData,
                    CommandText = selectText
                };
                cmd.Parameters.Add("@ProcessDate", SqlDbType.Date);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters["@Vendor"].Value = Vendor;

                SqlDataReader dr = null;
                dr = cmd.ExecuteReader();
                using (StreamWriter file = new StreamWriter(filename, true))
                {
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            string ProcessDate = GetColString(dr, "ChangeDate");
                            if (ProcessDate.Length > 0)
                                ProcessDate = DateHelper.ConvertToYYYYMMDD(ProcessDate);
                            ProcessDate = ProcessDate.PadRight(8 + 1);
                            string ChangeType = "Update";
                            ChangeType = ChangeType.PadRight(6 + 1);

                            string Cusip = GetColString(dr, "OldSymbol");
                            bool isCusipChange = false;
                            if(Cusip.Length.Equals(8))
                            {
                                isCusipChange = true;
                                Cusip = Cusip + CalculateCheckDigit(Cusip);
                                Cusip = Cusip.PadRight(10 + 1);
                            }
                            else
                            {
                                Cusip = "";
                                Cusip = Cusip.PadRight(10 + 1);
                            }

                            string CusipNew = GetColString(dr, "NewSymbol");
                            if(CusipNew.Length.Equals(8))
                            {
                                CusipNew = CusipNew + CalculateCheckDigit(CusipNew);
                                CusipNew = CusipNew.PadRight(10 + 2);
                            }
                            else
                            {
                                CusipNew = "";
                                CusipNew = CusipNew.PadRight(10 + 2);
                            }

                            string Ticker = "";
                            string TickerNew = "";

                            if( isCusipChange)
                            {
                                Ticker = Ticker.PadRight(10 + 1);
                                TickerNew = TickerNew.PadRight(10 + 1);
                            }
                            else
                            {
                                Ticker = GetColString(dr, "OldSymbol");
                                Ticker = Ticker.PadRight(10 + 1);
                                TickerNew = GetColString(dr, "NewSymbol");
                                TickerNew = TickerNew.PadRight(10 + 1);
                            }


                            string CompanyName = "";
                            CompanyName = CompanyName.PadRight(25 + 1);
                            string CompanyNameNew = GetColString(dr, "CompanyName");
                            CompanyNameNew = CompanyNameNew.PadRight(25 + 1);
                            string SectorCode = "";
                            SectorCode = SectorCode.PadRight(8 + 1);
                            string SectorCodeNew = "";
                            SectorCodeNew = SectorCodeNew.PadRight(8 + 3);
                            string Exchange = "";
                            Exchange = Exchange.PadRight(12 + 1);
                            string ExchangeNew = "";
                            ExchangeNew = ExchangeNew.PadRight(12 + 1);

                            file.WriteLine(ProcessDate + ChangeType + Cusip + CusipNew + Ticker + TickerNew + CompanyName + CompanyNameNew + SectorCode + SectorCodeNew
                                           + Exchange + ExchangeNew);
                        }
                    }
                }
                dr.Close();
                /////////////////////////////////
#if Commented
                selectText = @"
                SELECT *
                  FROM HistoricalSecurityMasterFullChanges
                  WHERE 
	                ProcessDate = @ProcessDate
	                and (id not in (
	                  SELECT id
                      FROM HistoricalSecurityMasterFullChanges
                      WHERE 
	                  ProcessDate = @ProcessDate
	                  and (
	                     (cusip in ( SELECT OldSymbol FROM HistoricalSymbolChanges WHERE ChangeDate = @ProcessDate ))
	                     or (Ticker in ( SELECT OldSymbol FROM HistoricalSymbolChanges WHERE ChangeDate = @ProcessDate ))
		                 or (CusipNew in ( SELECT NewSymbol FROM HistoricalSymbolChanges WHERE ChangeDate = @ProcessDate ))
		                 or (TickerNew in ( SELECT NewSymbol FROM HistoricalSymbolChanges WHERE ChangeDate = @ProcessDate ))
		                 )
	                ))
                ";
#endif
                selectText = @"
                SELECT *
                  FROM HistoricalSecurityMasterFullChanges
                  WHERE 
	                ProcessDate = @ProcessDate and Vendor = @Vendor
                  ORDER BY ChangeType desc
                ";
                cmd.CommandText = selectText;

                dr = null;
                dr = cmd.ExecuteReader();
                using(StreamWriter file = new StreamWriter(filename, true))
                {
                    if(dr.HasRows)
                    {
                        while(dr.Read())
                        {
                            string ProcessDate = GetColString(dr, "ProcessDate");
                            if(ProcessDate.Length > 0)
                                ProcessDate = DateHelper.ConvertToYYYYMMDD(ProcessDate);
                            ProcessDate = ProcessDate.PadRight(8 + 1);
                            string ChangeType = GetColString(dr, "ChangeType");
                            ChangeType = ChangeType.PadRight(6 + 1);
                            string Cusip = GetColString(dr, "Cusip");
                            Cusip = Cusip + CalculateCheckDigit(Cusip);
                            Cusip = Cusip.PadRight(10 + 1);
                            string CusipNew = GetColString(dr, "CusipNew");
                            CusipNew = CusipNew + CalculateCheckDigit(CusipNew);
                            CusipNew = CusipNew.PadRight(10 + 2);
                            string Ticker = GetColString(dr, "Ticker");
                            Ticker = Ticker.PadRight(10 + 1);
                            string TickerNew = GetColString(dr, "TickerNew");
                            TickerNew = TickerNew.PadRight(10 + 1);
                            string CompanyName = GetColString(dr, "CompanyName");
                            CompanyName = CompanyName.PadRight(25 + 1);
                            string CompanyNameNew = GetColString(dr, "CompanyNameNew");
                            CompanyNameNew = CompanyNameNew.PadRight(25 + 1);
                            string SectorCode = GetColString(dr, "SectorCode");
                            SectorCode = SectorCode.PadRight(8 + 1);
                            string SectorCodeNew = GetColString(dr, "SectorCodeNew");
                            SectorCodeNew = SectorCodeNew.PadRight(8 + 4);
                            string Exchange = GetColString(dr, "Exchange");
                            Exchange = Exchange.PadRight(12 + 1);
                            string ExchangeNew = GetColString(dr, "ExchangeNew");
                            ExchangeNew = ExchangeNew.PadRight(12 + 1);

                            file.WriteLine(ProcessDate + ChangeType + Cusip + CusipNew + Ticker + TickerNew + CompanyName + CompanyNameNew + SectorCode + SectorCodeNew
                                           + Exchange + ExchangeNew);
                        }
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

        public static string CalculateCheckDigit(string cusip)
        {
            int sum = 0;
            char[] digits = cusip.ToUpper().ToCharArray();
            string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#";
            string checkDigit = "";

            if(cusip.Length.Equals(8))
            {
                for(int i = 0; i < digits.Length; i++)
                {
                    int val;
                    if(!int.TryParse(digits[i].ToString(), out val))
                        val = alphabet.IndexOf(digits[i]) + 10;

                    if((i % 2) != 0)
                        val *= 2;

                    val = (val % 10) + (val / 10);

                    sum += val;
                }

                int check = (10 - (sum % 10)) % 10;
                checkDigit = check.ToString();
            }

            return(checkDigit);
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

                            sourceFilename = sourceDir + "4rl_" + DateHelper.ConvertToYYYYMMDD(sFileDate) + "_RefData.txt";

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
