using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using AdventUtilityLibrary;


namespace IndexDataEngineLibrary
{
    public static class ProcessStatus
    {
        public enum WhichStatus
        {
            OpenData,
            CloseData,
            TotalReturnData,
            SecurityMasterData,
            SymbolChangeData,
            AxmlConstituentData,
            AxmlSectorData,
            ExpectedConstituentClientFiles,
            ActualConstituentClientFiles,
            ExpectedSectorClientFiles,
            ActualSectorClientFiles
        }

        public enum StatusValue
        {
            Unassigned,
            AssignToPass,
            Pass,
            Fail,
            IgnoreArgument
        }


        public static string ConnectionString { get; set; }

        public static bool UseProcessStatus { get; set; }

        public static int ExpectedConstituentClientFiles { get; set; }
        public static int ActualConstituentClientFiles { get; set; }
        public static int ExpectedSectorClientFiles { get; set; }
        public static int ActualSectorClientFiles { get; set; }


        private static SqlConnection mSqlConn = null;

        private static void OpenSqlConn()
        {
            if ((ConnectionString.Length > 0) && (mSqlConn == null))
            {
                mSqlConn = new SqlConnection(ConnectionString);
                mSqlConn.Open();
            }
        }

        public static void Initialize()
        {
            UseProcessStatus = true;
        }

        public static void Add( string sProcessDate, string Vendor, string Dataset, string IndexName)
        {
            if (UseProcessStatus)
            {
                try
                {
                    if (mSqlConn == null)
                    {
                        OpenSqlConn();
                    }
                    string Sql = @"
                    delete from ProcessStatus
                    where ProcessDate = @ProcessDate 
                    and Vendor = @Vendor
                    and Dataset = @Dataset
                    and IndexName = @IndexName 
                    ";
                    SqlCommand cmd = new SqlCommand(Sql, mSqlConn);
                    cmd.Parameters.Add("@ProcessDate", SqlDbType.DateTime);
                    cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                    cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                    cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                    cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                    cmd.Parameters["@Vendor"].Value = Vendor;
                    cmd.Parameters["@Dataset"].Value = Dataset;
                    cmd.Parameters["@IndexName"].Value = IndexName;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText =
                        "insert into ProcessStatus (ProcessDate, Vendor, Dataset, IndexName) " +
                        "Values (@ProcessDate, @Vendor, @Dataset, @IndexName)";
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
        }

        public static void DeleteOldEntries(string sProcessDate)
        {
            int daysOfHistory = 0;
            try
            {
                    
                var v = AppSettings.Get<int>("processStatusDaysOfHistory");
                if (v.Equals(0))
                    daysOfHistory = 30;
                else
                    daysOfHistory = v;

                daysOfHistory *= -1;

                DateTime date = DateTime.Parse(sProcessDate);
                date = date.AddDays(daysOfHistory);

                if (mSqlConn == null)
                {
                    OpenSqlConn();
                }
                string Sql = @"
                delete from ProcessStatus
                where ProcessDate < @ProcessDate 
                ";
                SqlCommand cmd = new SqlCommand(Sql, mSqlConn);
                cmd.Parameters.Add("@ProcessDate", SqlDbType.DateTime);
                cmd.Parameters["@ProcessDate"].Value = date;
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
                LogHelper.WriteLine( "deleted " + daysOfHistory + " from ProcessStatus table" );
            }
        }


        public static void Update(DateTime ProcessDate, string Vendor, string Dataset, string IndexName, WhichStatus whichStatus, StatusValue statusValue)
        {
            Update(ProcessDate.ToString("MM/dd/yyy"), Vendor, Dataset, IndexName, whichStatus, statusValue);
        }

        public static void Update(string sProcessDate, string Vendor, string Dataset, string IndexName, WhichStatus whichStatus, StatusValue statusValue)
        {
            if (UseProcessStatus)
            {
                try
                {
                    if (mSqlConn == null)
                    {
                        OpenSqlConn();
                    }
                    string column = "";
                    string columnValue = "";

                    column = whichStatus.ToString();
                    if (whichStatus.Equals(WhichStatus.ExpectedConstituentClientFiles))
                    {
                        columnValue = ExpectedConstituentClientFiles.ToString();
                    }
                    else if (whichStatus.Equals(WhichStatus.ActualConstituentClientFiles))
                    {
                        columnValue = ActualConstituentClientFiles.ToString();
                    }
                    else if (whichStatus.Equals(WhichStatus.ExpectedSectorClientFiles))
                    {
                        columnValue = ExpectedSectorClientFiles.ToString();
                    }
                    else if (whichStatus.Equals(WhichStatus.ActualSectorClientFiles))
                    {
                        columnValue = ActualSectorClientFiles.ToString();
                    }
                    else
                    {
                        columnValue = statusValue.ToString();
                        columnValue = columnValue.Substring(0, 1);
                    }

                    string Sql = "update ProcessStatus set " + column + "='" + columnValue + "' ";
                    string SqlWhere = "";
                    SqlCommand cmd = new SqlCommand(Sql + SqlWhere, mSqlConn);

                    if (IndexName.Length > 0)
                    {
                        SqlWhere = @"
                        where ProcessDate = @ProcessDate 
                        and Vendor = @Vendor
                        and Dataset = @Dataset
                        and IndexName = @IndexName 
                        ";
                        cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                        cmd.Parameters["@IndexName"].Value = IndexName;
                    }
                    else
                    {
                        SqlWhere = @"
                        where ProcessDate = @ProcessDate 
                        and Vendor = @Vendor
                        and Dataset = @Dataset
                        ";
                    }

                    cmd.CommandText = Sql + SqlWhere;
                    cmd.Parameters.Add("@ProcessDate", SqlDbType.DateTime);
                    cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                    cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                    cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                    cmd.Parameters["@Vendor"].Value = Vendor;
                    cmd.Parameters["@Dataset"].Value = Dataset;
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
        }

        public static bool GenerateReturns(string sProcessDate, string Vendor, string Dataset, string IndexName)
        {
            bool GenerateReturns = false;
            if (UseProcessStatus)
            {
                List<string> dataSets = new List<string>();
                List<string> indexNames = new List<string>();


                if (Dataset.Equals("sp900"))
                {
                    dataSets.Add("sp400");
                    dataSets.Add("sp500");
                    indexNames.Add("sp400");
                    indexNames.Add("sp500");
                }
                else if (Dataset.Equals("sp1000"))
                {
                    dataSets.Add("sp400");
                    dataSets.Add("sp600");
                    indexNames.Add("sp400");
                    indexNames.Add("sp600");
                }
                else if (Dataset.Equals("sp1500"))
                {
                    dataSets.Add("sp400");
                    dataSets.Add("sp500");
                    dataSets.Add("sp600");
                    indexNames.Add("sp400");
                    indexNames.Add("sp500");
                    indexNames.Add("sp600");
                }
                else
                {
                    dataSets.Add(Dataset);
                    indexNames.Add(IndexName);
                }

                int i = 0;
                foreach (string dataSet in dataSets)
                {
                    try
                    {
                        if (mSqlConn == null)
                        {
                            OpenSqlConn();
                        }

                        string Sql = @"
                        select * from ProcessStatus
                        where ProcessDate = @ProcessDate 
                        and Vendor = @Vendor
                        and Dataset = @Dataset
                        and IndexName = @IndexName 
                        ";

                        SqlCommand cmd = new SqlCommand(Sql, mSqlConn);
                        cmd.Parameters.Add("@ProcessDate", SqlDbType.DateTime);
                        cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                        cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                        cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                        cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                        cmd.Parameters["@Vendor"].Value = Vendor;
                        cmd.Parameters["@Dataset"].Value = dataSet;
                        cmd.Parameters["@IndexName"].Value = indexNames[i];
                        SqlDataReader dr = cmd.ExecuteReader();
                        if (dr.HasRows)
                        {
                            if (dr.Read())
                            {
                                bool bOpenData = dr["OpenData"].ToString().Equals("P");
                                bool bCloseData = dr["CloseData"].ToString().Equals("P");
                                bool bTotalReturnData = dr["TotalReturnData"].ToString().Equals("P");
                                // JK:ToDo bool bSecurityMasterData = dr["SecurityMasterData"].ToString().Equals("P");
                                bool bSymbolChangeData = dr["SymbolChangeData"].ToString().Equals("P");
                                if (Vendor.Equals(Vendors.Russell.ToString()))
                                {
                                    GenerateReturns = (bOpenData.Equals(true) && bCloseData.Equals(true)
                                                       && bTotalReturnData.Equals(true) && bSymbolChangeData.Equals(true));
                                }
                                else if (Vendor.Equals(Vendors.Snp.ToString()))
                                {
                                    GenerateReturns = (bOpenData.Equals(true) && bCloseData.Equals(true));
                                }
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
                    }
                    if (GenerateReturns.Equals(false))
                        break;
                    i += 1;
                }
            }
            return (GenerateReturns);
        }

        public static StatusValue CheckStatus(string sProcessDate, string Vendor, string Dataset, string IndexName, WhichStatus whichStatus)
        {
            StatusValue checkStatus = StatusValue.Unassigned;
            StatusValue checkStatus400 = StatusValue.Unassigned;
            StatusValue checkStatus500 = StatusValue.Unassigned;
            StatusValue checkStatus600 = StatusValue.Unassigned;

            try
            {
                if (mSqlConn == null)
                {
                    OpenSqlConn();
                }

                string Sql = @"
                    select * from ProcessStatus
                    where ProcessDate = @ProcessDate 
                    and Vendor = @Vendor
                    and Dataset = @Dataset
                    and IndexName = @IndexName 
                    ";

                SqlCommand cmd = new SqlCommand(Sql, mSqlConn);
                cmd.Parameters.Add("@ProcessDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@Vendor", SqlDbType.VarChar);
                cmd.Parameters.Add("@Dataset", SqlDbType.VarChar);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters["@ProcessDate"].Value = sProcessDate;
                cmd.Parameters["@Vendor"].Value = Vendor;
                cmd.Parameters["@Dataset"].Value = Dataset;
                cmd.Parameters["@IndexName"].Value = IndexName;
                SqlDataReader dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    if (dr.Read())
                    {
                        if (whichStatus.Equals(WhichStatus.OpenData))
                        {
                            if (dr["OpenData"].ToString().Equals("P"))
                                checkStatus = StatusValue.Pass;
                            else if (dr["OpenData"].ToString().Equals(""))
                                checkStatus = StatusValue.Unassigned;
                        }
                        else if (whichStatus.Equals(WhichStatus.CloseData))
                        {
                            if (dr["CloseData"].ToString().Equals("P"))
                                checkStatus = StatusValue.Pass;
                            else if (dr["CloseData"].ToString().Equals(""))
                                checkStatus = StatusValue.Unassigned;
                        }
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
            }

            if (checkStatus.Equals(ProcessStatus.StatusValue.Unassigned))
            {
                if (Dataset.Equals("sp900"))
                {
                    checkStatus400 = CheckStatus(sProcessDate, Vendor, "sp400", "sp400", whichStatus);
                    checkStatus500 = CheckStatus(sProcessDate, Vendor, "sp500", "sp500", whichStatus);
                    if (checkStatus400.Equals(StatusValue.Pass) && checkStatus500.Equals(StatusValue.Pass))
                        checkStatus = StatusValue.AssignToPass;
                }
                else if (Dataset.Equals("sp1000"))
                {
                    checkStatus400 = CheckStatus(sProcessDate, Vendor, "sp400", "sp400", whichStatus);
                    checkStatus600 = CheckStatus(sProcessDate, Vendor, "sp600", "sp600", whichStatus);
                    if (checkStatus400.Equals(StatusValue.Pass) && checkStatus600.Equals(StatusValue.Pass))
                        checkStatus = StatusValue.AssignToPass;
                }
                else if (Dataset.Equals("sp1500"))
                {
                    checkStatus400 = CheckStatus(sProcessDate, Vendor, "sp400", "sp400", whichStatus);
                    checkStatus500 = CheckStatus(sProcessDate, Vendor, "sp500", "sp500", whichStatus);
                    checkStatus600 = CheckStatus(sProcessDate, Vendor, "sp600", "sp600", whichStatus);
                    if (checkStatus400.Equals(StatusValue.Pass) && checkStatus500.Equals(StatusValue.Pass) && checkStatus600.Equals(StatusValue.Pass))
                        checkStatus = StatusValue.AssignToPass;
                }

            }
            return (checkStatus);
        }
    }
}
