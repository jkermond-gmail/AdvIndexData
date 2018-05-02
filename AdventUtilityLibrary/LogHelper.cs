using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace AdventUtilityLibrary
{
    public class LogHelper
    {
        public LogHelper()
        {

        }
        public static void Error(string message, string module)
        {
            WriteEntry(message, "error", module);
        }

        public static void Error(Exception ex, string module)
        {
            WriteEntry(ex.Message, "error", module);
        }

        public static void Warning(string message, string module)
        {
            WriteEntry(message, "warning", module);
        }

        public void Info(string message, string module)
        {
            WriteEntry(message, "info", module);
        }

        public void WriteLine(string message)
        {
            Trace.WriteLine(message);
            Console.WriteLine(message);
        }


        private static void WriteEntry(string message, string type, string module)
        {
            string formattedMsg = string.Format("{0},{1},{2},{3}",
                                  DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                                  type,
                                  module,
                                  message);
            Trace.WriteLine(formattedMsg);
            Console.WriteLine(formattedMsg);
        }

    }
}
