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
                    LogHelper.WriteLine(ex.Message);
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

        public static string PrevBusinessDayMMDDYYYY_Slash(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            date = PrevBusinessDay(date);
            return (date.ToString("MM/dd/yyyy"));
        }


        public static string ConvertToYYYYMMDD(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            return (date.ToString("yyyyMMdd"));
        }

        public static string ConvertToYYYYMMDD(DateTime Date)
        {
            return (Date.ToString("yyyyMMdd"));
        }

        public static DateTime PrevEndOfMonthDay(DateTime date)
        {
            DateTime eomDate = DateTime.MinValue;

            if ( date.Month > 1)
                eomDate = new DateTime(date.Year, date.Month - 1, 1);
            else
                eomDate = new DateTime(date.Year - 1, 12, 1);

            return (eomDate.AddMonths(1).AddDays(-1));
        }

        public static DateTime PrevEndOfMonthDay(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            return (PrevEndOfMonthDay(date));
        }

        public static DateTime EndOfMonthDay(DateTime date)
        {
            DateTime eomDate = DateTime.MinValue;

            if (date.Month < 12)
                eomDate = new DateTime(date.Year, date.Month + 1, 1);
            else
                eomDate = new DateTime(date.Year + 1, 1, 1);

            return (eomDate.AddDays(-1));
        }

        public static DateTime EndOfMonthDay(string sDate)
        {
            DateTime date = DateTime.Parse(sDate);
            return (EndOfMonthDay(date));
        }

        public static bool IsPrevEndofMonthOnWeekend(DateTime Date)
        {
            return(IsPrevEndofMonthOnWeekend(Date.ToString("MM/dd/yyyy")));
        }

        public static bool IsEndofMonthOnWeekend(DateTime Date)
        {
            return (IsEndofMonthOnWeekend(Date.ToString("MM/dd/yyyy")));
        }

        public static bool IsPrevEndofMonthOnWeekend(string sDate)
        {
            bool isOnWeekend = false;

            DateTime businessDay = DateTime.Parse(sDate);
            DateTime prevBusinessDay = PrevBusinessDay(businessDay);
            DateTime prevEndOfMonthDay = PrevEndOfMonthDay(sDate);

            if ((prevEndOfMonthDay > prevBusinessDay) && !IsWeekday(prevEndOfMonthDay))
            { 
                isOnWeekend = true;
                //LogHelper.WriteLine("BizDay," + businessDay.ToShortDateString()
                //    + ",PrevEndOfMonthDay," + prevEndOfMonthDay.ToShortDateString()
                //    + ",PrevBizDay," + prevBusinessDay.ToShortDateString());

            }

            return (isOnWeekend);
        }

        public static bool IsEndofMonthOnWeekend(string sDate)
        {
            bool isOnWeekend = false;

            DateTime businessDay = DateTime.Parse(sDate);
            DateTime nextBusinessDay = NextBusinessDay(businessDay);
            DateTime endOfMonthDay = EndOfMonthDay(sDate);

            if ((endOfMonthDay < nextBusinessDay) && !IsWeekday(endOfMonthDay))
            {
                isOnWeekend = true;
            }
            return (isOnWeekend);
        }
    }
}
