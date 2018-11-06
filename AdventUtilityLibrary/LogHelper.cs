using System;
using System.Diagnostics;
using System.IO;

namespace AdventUtilityLibrary
{
    public static class LogHelper
    {
        private static StreamWriter swLogFile = null;
        private static string LogFileName;
        //private static string OutputPath = @"C:\A_Development\visual studio 2017\Projects\AdvIndexData\IndexDataForm\Output\";
        private static bool LogFileOpened = false;

        /*
        private static void StartLog()
        {
            LogFileName = OutputPath + "AdvIndexDataLog.txt";
            if (File.Exists(LogFileName))
                File.Delete(LogFileName);
            swLogFile = File.CreateText(LogFileName);
            swLogFile.WriteLine(LogFileName + DateTime.Now);
            swLogFile.Flush();
        }
        */

        public static void StartLog(string logFileName, string logFilePath, bool deleteExisting)
        {
            if (LogFileOpened)
                CloseAndFlush(ref swLogFile);
            LogFileName = Path.Combine(logFilePath, logFileName);
            if (deleteExisting && File.Exists(LogFileName))
            {
                File.Delete(LogFileName);
                swLogFile = new StreamWriter(Path.Combine(logFilePath, logFileName));
            }
            else if( !File.Exists(LogFileName))
            {
                swLogFile = new StreamWriter(Path.Combine(logFilePath, logFileName));
            }
            else
            {
                bool append = true;
                swLogFile = new StreamWriter(Path.Combine(logFilePath, logFileName), append);
            }
            LogFileOpened = true;
            string message = LogFileName + " opened " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            //WriteLine(message);
            swLogFile.Flush();
        }

        private static void EndLog()
        {
            if (swLogFile.BaseStream != null)
            {
                swLogFile.Flush();
                swLogFile.Close();
            }
        }

        private static void CloseAndFlush(ref StreamWriter WriteFile)
        {
            if (WriteFile.BaseStream != null)
            {
                WriteFile.Flush();
                WriteFile.Close();
                WriteFile = null;
                LogFileOpened = false;
            }
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

        public static void Info(string message, string module)
        {
            WriteEntry(message, "info", module);
        }

        public static void WriteLine(string message)
        {
            //Trace.WriteLine(message);
            //Trace.TraceInformation(message);
            //Trace.Flush();
            Console.WriteLine(message);

            string formattedMsg = string.Format("{0}:{1}",
                                  DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                                  message);


            swLogFile.WriteLine(formattedMsg);
            swLogFile.Flush();
        }


        private static void WriteEntry(string message, string type, string module)
        {
            string formattedMsg = string.Format("{0},{1},{2},{3}",
                                  DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                                  type,
                                  module,
                                  message);
            //Trace.WriteLine(formattedMsg);
            //Trace.TraceInformation(formattedMsg);
            //Trace.Flush();
            //Console.WriteLine(formattedMsg);
            WriteLine(message);
        }

    }
}
