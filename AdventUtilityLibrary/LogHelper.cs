using System;
using System.Diagnostics;
using System.IO;

namespace AdventUtilityLibrary
{
    public static class LogHelper
    {
        private static StreamWriter swLogFile = null;
        private static string mLogFileNameWithPath;
        private static string mLogFileNameOnly;
        private static string mLogFilePath;        
        private static bool mLogFileOpened = false;

        /* ToDo

        '/////////////////////////////////////////////////////////////////////////////
        ' DeleteOldLogFiles -
        '/////////////////////////////////////////////////////////////////////////////
        Public Sub DeleteOldLogFiles(ByVal vPath, ByVal vDate)
            Dim vFileDate, vCriteriaDay, vDatePos
            Dim oArchiveFolder As Scripting.Folder
            Dim oLogFiles As Scripting.Files
            Dim oLogFile As Scripting.File
            On Error GoTo ErrorHandler
    
            Call gHCU.VerifyFolder(vPath)
            Set oArchiveFolder = gFSO.GetFolder(vPath)
            Set oLogFiles = oArchiveFolder.Files
            For Each oLogFile In oLogFiles
                ' Format of filename is: FILENAME.YYYYMMDD.log. Therefore:
                vDatePos = Len(oLogFile.Name) - 11
                vFileDate = gU.ConvertDate(Mid(oLogFile.Name, vDatePos, 8), "YYYYMMDD")
                vCriteriaDay = DateDiff("y", vFileDate, vDate)
                If vCriteriaDay >= 30 Then
                    Err.Number = eeMessage
                    Call gErr.Log("Archive Log file " & oLogFile.Name & " deleted.")
                    Call gFSO.DeleteFile(vPath & "\" & oLogFile.Name)
                End If
            Next

        Exit Sub
        ErrorHandler:
            Call gErr.Log("DeleteOldLogFiles()")
            Call Err.Raise(eeError)
        End Sub



        */

        public static void StartLog()
        {
            mLogFileNameOnly = AppSettings.Get<string>("logFileName");
            mLogFilePath = AppSettings.Get<string>("logFilePath");
            bool deleteExisting = AppSettings.Get<bool>("deleteLog");

            if (mLogFileOpened)
                CloseAndFlush(ref swLogFile);

            mLogFileNameWithPath = Path.Combine(mLogFilePath, mLogFileNameOnly);
            if (deleteExisting && File.Exists(mLogFileNameWithPath))
            {
                File.Delete(mLogFileNameWithPath);
                swLogFile = new StreamWriter(mLogFileNameWithPath);
            }
            else if( !File.Exists(mLogFileNameWithPath))
            {
                swLogFile = new StreamWriter(mLogFileNameWithPath);
            }
            else
            {
                bool append = true;
                swLogFile = new StreamWriter(mLogFileNameWithPath, append);
            }
            mLogFileOpened = true;
            string message = mLogFileNameWithPath + " opened " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
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
                mLogFileOpened = false;
            }
        }

        public static void Flush()
        {
            if (swLogFile.BaseStream != null)
            {
                swLogFile.Flush();
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

        public static void ArchiveLog( DateTime ArchiveDate)
        {
            EndLog();
            string archiveFilename = mLogFileNameOnly.Substring(0, mLogFileNameOnly.Length - 4);
            archiveFilename += "." + ArchiveDate.ToString("yyyyMMdd") + ".txt";
            archiveFilename = Path.Combine(mLogFilePath, "LogFileArchive", archiveFilename);

            if (File.Exists(mLogFileNameWithPath) && !File.Exists(archiveFilename))
            { 
                File.Copy(mLogFileNameWithPath, archiveFilename);
                File.Delete(mLogFileNameWithPath);
            }

            StartLog();
        }
    }
}
