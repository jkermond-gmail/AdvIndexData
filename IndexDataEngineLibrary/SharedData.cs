using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using AdventUtilityLibrary;
using System.Configuration;


namespace IndexDataEngineLibrary
{
    public enum Vendors
    {
        Russell,
        Snp
    }

    public enum AdventOutputType
    {
        Constituent,
        Sector
    }

    public struct IndexReturnStruct
    {
        public double IndexReturn1;
        public double IndexReturn2;
        public string IndexDate;
    }


    internal sealed class SharedData
    {
        //private string mConnectionString = "server=VSTGMDDB2-1;database=IndexData;uid=sa;pwd=M@gichat!";
        //private string mConnectionString = @"server=JKERMOND-NEW\SQLEXPRESS2014;database=IndexData;uid=sa;pwd=M@gichat!";
        private string mConnectionStringAmdVifs = null;
        private string mConnectionStringIndexData = null;

        private SqlConnection mSqlConn = null;


        Vendors mVendor;

        internal Vendors Vendor
        {
            get { return mVendor; }
            set { mVendor = value; }
        }

        public string ConnectionStringIndexData
        {
            get { return mConnectionStringIndexData; }
        }

        public string ConnectionStringAmdVifs
        {
            get { return mConnectionStringAmdVifs; }
        }

        public SharedData()
        {
            mConnectionStringIndexData = ConfigurationManager.ConnectionStrings["dbConnectionIndexData"].ConnectionString;
            mConnectionStringAmdVifs = ConfigurationManager.ConnectionStrings["dbConnectionAmdVifs"].ConnectionString;
        }


        public string[] GetIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(mConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                string SqlSelectCount = "select count(AdventIndexName) from VendorIndexMap ";
                string SqlSelect = "select AdventIndexName from VendorIndexMap ";
                string SqlWhere = "";
                if (mVendor.Equals(Vendors.Russell))
                    SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                else if (mVendor.Equals(Vendors.Snp))
                    SqlWhere = "where Vendor = 'StandardAndPoors' and Supported = 'Yes' ";
                string SqlOrderBy = "order by AdventIndexName";

                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    Indices = new string[count];
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        Indices[i] = dr["AdventIndexName"].ToString();
                        i += 1;
                    }
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
            }

            return (Indices);
        }

        public string[] GetVendorIndices()
        {
            string[] Indices = null;
            SqlConnection conn = new SqlConnection(mConnectionStringIndexData);
            SqlDataReader dr = null;

            try
            {
                string SqlSelectCount = "select count(AdventIndexName) from VendorIndexMap ";
                string SqlSelect = "select AdventIndexName from VendorIndexMap ";
                string SqlWhere = "";
                if (mVendor.Equals(Vendors.Russell))
                    SqlWhere = "where Vendor = 'Russell' and Supported = 'Yes' ";
                else if (mVendor.Equals(Vendors.Snp))
                    SqlWhere = "where Vendor = 'StandardAndPoors' and Supported = 'Yes' ";
                string SqlOrderBy = "order by AdventIndexName";

                conn.Open();
                SqlCommand cmd = new SqlCommand(SqlSelectCount + SqlWhere, conn);
                int count = (int)cmd.ExecuteScalar();
                if (count > 0)
                {
                    Indices = new string[count];
                    cmd.CommandText = SqlSelect + SqlWhere + SqlOrderBy;
                    dr = cmd.ExecuteReader();
                    int i = 0;
                    while (dr.Read())
                    {
                        Indices[i] = dr["AdventIndexName"].ToString();
                        i += 1;
                    }
                }
            }
            catch (SqlException ex)
            {
                LogHelper.WriteLine(ex.Message);
            }

            finally
            {
                dr.Close();
                conn.Close();
            }

            return (Indices);
        }

        public void AddTotalReturn(string sDate, string sIndexName, string sVendorFormat,
                                   double dReturn, string sWhichReturn)
        {
            DateTime oDate = DateTime.Parse(sDate);
            AddTotalReturn(oDate, sIndexName, sVendorFormat, dReturn, sWhichReturn);
        }


        public void AddTotalReturn(DateTime oDate, string sIndexName, string sVendorFormat,
                                   double dReturn, string sWhichReturn)
        {
            try
            {
                if (mSqlConn == null)
                {
                    mSqlConn = new SqlConnection(mConnectionStringIndexData);
                    mSqlConn.Open();
                }
                string SqlSelect = @"
                    select count(*) from TotalReturns
                    where IndexName = @IndexName 
                    and ReturnDate = @ReturnDate 
                    and VendorFormat = @VendorFormat
                    ";
                SqlCommand cmd = new SqlCommand(SqlSelect, mSqlConn);
                cmd.Parameters.Add("@IndexName", SqlDbType.VarChar);
                cmd.Parameters.Add("@ReturnDate", SqlDbType.DateTime);
                cmd.Parameters.Add("@VendorFormat", SqlDbType.VarChar);
                cmd.Parameters["@IndexName"].Value = sIndexName;
                cmd.Parameters["@ReturnDate"].Value = oDate;
                cmd.Parameters["@VendorFormat"].Value = sVendorFormat;
                int iCount = (int)cmd.ExecuteScalar();
                if (iCount == 0)
                {
                    cmd.CommandText =
                        "insert into TotalReturns (IndexName, ReturnDate, VendorFormat, " + sWhichReturn + ") " +
                        "Values (@IndexName, @ReturnDate, @VendorFormat, @" + sWhichReturn + ")";
                }
                else
                {
                    cmd.CommandText =
                        "update TotalReturns set " + sWhichReturn + " = @" + sWhichReturn + " " +
                        "where IndexName = @IndexName and ReturnDate = @ReturnDate and VendorFormat = @VendorFormat";
                }
                cmd.Parameters.Add("@" + sWhichReturn, SqlDbType.Float, 8);
                cmd.Parameters["@" + sWhichReturn].Value = dReturn;
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
