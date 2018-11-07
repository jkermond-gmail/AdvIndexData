using System;
using System.Data.SqlClient;
using System.Data;

/*
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
*/
namespace AdventUtilityLibrary
{
    public static class DateHelper
    {
        public static string ConnectionString { get; set; }

        private static SqlConnection mSqlConn = null;

        private static void OpenSqlConn()
        {
            if ((ConnectionString.Length > 0) && (mSqlConn == null))
            {
                mSqlConn = new SqlConnection(ConnectionString);
                mSqlConn.Open();
            }
        }

        public static bool IsWeekday(DateTime date)
        {
            DayOfWeek dow = date.DayOfWeek;
            bool bIs = ((int)dow >= 1 && (int)dow <= 5);
            return (bIs);
        }

        public static bool IsHoliday(DateTime date)
        {
            bool bIs = false;

            try
            {
                if (mSqlConn == null)
                    OpenSqlConn();
                string SqlSelect = @"
                    SELECT count(HDate)
                    FROM Holidays
                    WHERE HDate = @HDate
                    ";

                SqlCommand cmd1 = new SqlCommand(SqlSelect, mSqlConn);
                cmd1.Parameters.Add("@HDate", SqlDbType.DateTime);
                cmd1.Parameters["@HDate"].Value = date.ToShortDateString();
                int iCount = (int)cmd1.ExecuteScalar();
                bIs = (iCount == 1);
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
            return (bIs);
        }

        public static bool IsBusinessDay(DateTime date)
        {
            bool bIs;

            if (!IsWeekday(date) || IsHoliday(date))
                bIs = false;
            else
                bIs = true;

            return (bIs);
        }

        public static DateTime NextBusinessDay(DateTime date)
        {
            date = date.AddDays(1);
            while (!IsBusinessDay(date))
                date = date.AddDays(1);
            return (date);
        }

        public static DateTime PrevBusinessDay(DateTime date)
        {
            date = date.AddDays(-1);
            while (!IsBusinessDay(date))
                date = date.AddDays(-1);
            return (date);
        }

        public static string PrevBusinessDay(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            date = PrevBusinessDay(date);
            return (date.ToString("yyyyMMdd"));
        }

        public static string ConvertToYYYYMMDD(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            return (date.ToString("yyyyMMdd"));
        }
    }
}
