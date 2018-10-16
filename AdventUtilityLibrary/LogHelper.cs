using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Configuration;

namespace AdventUtilityLibrary
{
    public class LogHelper
    {
        private StreamWriter swLogFile = null;
        private string LogFileName;
//        private string OutputPath = @"C:\A_Development\visual studio 2017\AdventProjects\IndexDataForm\IndexDataForm\Output\";
        private string OutputPath = @"";

        public LogHelper()
        {
            StartLog();
        }

        private void StartLog()
        {
            LogFileName = OutputPath + "AdvIndexDataLog.txt";
            if (File.Exists(LogFileName))
                File.Delete(LogFileName);
            swLogFile = File.CreateText(LogFileName);
            swLogFile.WriteLine(LogFileName + DateTime.Now);
            swLogFile.Flush();
        }

        private void StartLog(string logFileName)
        {
            if (swLogFile.BaseStream != null)
                CloseAndFlush(ref swLogFile);
            LogFileName = /* OutputPath + */ logFileName;
            if (File.Exists(LogFileName))
                File.Delete(LogFileName);
            swLogFile = File.CreateText(LogFileName);
            swLogFile.Flush();
        }

        private void EndLog()
        {
            if (swLogFile.BaseStream != null)
            {
                swLogFile.Flush();
                swLogFile.Close();
            }
        }

        private void CloseAndFlush(ref StreamWriter WriteFile)
        {
            if (WriteFile.BaseStream != null)
            {

                WriteFile.Flush();
                WriteFile.Close();
                WriteFile = null;
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

        public void Info(string message, string module)
        {
            WriteEntry(message, "info", module);
        }

        public void WriteLine(string message)
        {
            Trace.WriteLine(message);
            Trace.TraceInformation(message);
            Trace.Flush();
            Console.WriteLine(message);
            swLogFile.WriteLine(message);
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
            Trace.TraceInformation(formattedMsg);
            Trace.Flush();
            Console.WriteLine(formattedMsg);
        }

    }
}
