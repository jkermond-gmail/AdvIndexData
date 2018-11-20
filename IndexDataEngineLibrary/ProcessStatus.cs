using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;


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
            ExpectedClientFiles,
            ActualClientFiles
        }

        public enum StatusValue
        {
            Pass,
            Fail
        }


        public static string ConnectionString { get; set; }

        public static bool UseProcessStatus { get; set; }

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
                        Console.WriteLine(ex.Message);
                    }
                }
                finally
                {
                }
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
                    columnValue = statusValue.ToString();
                    columnValue = columnValue.Substring(0, 1);

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
                        Console.WriteLine(ex.Message);
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
                        Console.WriteLine(ex.Message);
                    }
                }
                finally
                {
                }
            }
            return (GenerateReturns);
        }

    }
}
