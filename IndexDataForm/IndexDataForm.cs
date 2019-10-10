﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Diagnostics;

using IndexDataEngineLibrary;
using AdventUtilityLibrary;


namespace IndexDataForm
{
    public partial class IndexDataForm : Form
    {
        internal int defaultTimerInterval = 1000 * 2;                           // 2 seconds
        internal int timerInterval = 1000 * 2;                                  // 2 seconds
        //internal int defaultTimerInterval = 1000 * 20;                        // 20 seconds
        //internal int defaultTimerInterval = 60 * 1000 * 2;                    // 2 min
        //internal int TIMER_HOLD_WHILE_WORKING_INTERVAL = 60 * 1000 * 200 ;    // 200 min

        //private System.Timers.Timer timer1 = new System.Timers.Timer();

        private IndexDataEngine indexDataEngine;
        private RussellData russellData = null;
        private SnpData snpData = null;

        public IndexDataForm()
        {
            InitializeComponent();
            //LogHelper.StartLog("IndexDataLog.txt", @"\IndexData\Logs\", deleteExisting);
            LogHelper.StartLog();

            russellData = new RussellData();
            snpData = new SnpData();
            var v = AppSettings.Get<int>("timerInterval");
            if (v.Equals(0))
                timerInterval = defaultTimerInterval;
            else
                timerInterval = v;
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            TimerEnableDisable("Starting Index Data Processing", true);
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            TimerEnableDisable("Stopping Index Data Processing", false);
        }


        private void timerRunIndexData_Tick(object sender, EventArgs e)
        {
            TimerEnableDisable("Index Data Engine Running", false);
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.Run();
            TimerEnableDisable("Index Data Engine Waiting " + timerInterval / 1000 + " seconds ", true);
        }

        private void TimerEnableDisable(string message, bool enable)
        {

            LogHelper.Info(message, "IndexDataForm");
            LogHelper.Flush();

            if (enable)
            {
                timerRunIndexData.Interval = timerInterval;
            }
            else
            {
            }
            timerRunIndexData.Enabled = enable;
            if (enable)
                timerRunIndexData.Start();
            else
            {
                timerRunIndexData.Stop();
            }
        }

        #region Russell tab page functionality

        private DateTime startDate;
        private DateTime endDate;
        private bool bStartDateSelected = false;
        private bool bEndDateSelected = false;

        private void tabPage1_Click(object sender, EventArgs e)
        {

            if (tabPage1.Text.Equals("Russell"))
            {
            }
            //else if (russellData != null)  // We are leaving tabRussellDb
            //  russellData.RussellData_Finish();

        }


        private void btnCalculateOneIndexReturns_Click(object sender, EventArgs e)
        {
            russellData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, (string)cbRussellIndices.SelectedItem);
        }

        private void btnCalculateAllIndexReturns_Click(object sender, EventArgs e)
        {
            string[] Indices = null;
            Indices = russellData.GetIndices();
            for (int i = 0; i < Indices.Length; i++)
            {
                string s = Indices[i];
                russellData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);
            }

        }
        private void btnGenerateReturns_Click(object sender, EventArgs e)
        {
            string Indexname = (string)cbRussellIndices.SelectedItem;
            if (cboVendor.SelectedItem.Equals("Russell"))
            {
                russellData.LogReturnData = chkLogReturnData.Checked;            

                if (cboOutputType.Text.Equals("Constituent"))
                    russellData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Constituent, chkHistoricalAxmlFile.Checked);
                else if (cboOutputType.Text.Equals("Sector"))
                    russellData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Sector, chkHistoricalAxmlFile.Checked);
            }
            else if (cboVendor.SelectedItem.Equals("Snp"))
            {
                if (cboOutputType.Text.Equals("Constituent"))
                    snpData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Constituent, chkHistoricalAxmlFile.Checked);
                else if (cboOutputType.Text.Equals("Sector"))
                    snpData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Sector, chkHistoricalAxmlFile.Checked);
            }

            //russellData.GenerateIndustryReturnsForDate(lnkEndDate.Text, (string)cbRussellIndices.SelectedItem);
        }


        private void btnUpdateRussellHoldings_Click(object sender, EventArgs e)
        {
            if (!bStartDateSelected)
                startDate = Convert.ToDateTime(lnkStartDate.Text);
            if (!bEndDateSelected)
                endDate = Convert.ToDateTime(lnkEndDate.Text);

            string DataSet = "All";

            if (cboVendor.SelectedItem.Equals("Russell"))
            {
                russellData.ProcessVendorFiles(startDate, endDate, DataSet, true, true, true, true, true);
            }
            else if (cboVendor.SelectedItem.Equals("Snp"))
            {
                snpData.ProcessVendorFiles(startDate, endDate, DataSet, true, true, true, true, true);
            }
        }

        private void btnCalculateTotalReturns_Click(object sender, EventArgs e)
        {
            string[] Indices = null;

            if (cboVendor.SelectedItem.Equals("Russell"))
            {
                Indices = russellData.GetIndices();
                for (int i = 0; i < Indices.Length; i++)
                {
                    string s = Indices[i];
                    russellData.CalculateVendorTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);
                    russellData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);
                    russellData.CalculateAdjustedTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);
                }

            }
            else if (cboVendor.SelectedItem.Equals("Snp"))
            {
                //Indices = snpData.GetIndices();
                //for (int i = 0; i < Indices.Length; i++)
                //{
                //    string s = Indices[i];
                //    snpData.CalculateVendorTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);
                //    snpData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, Indices[i]);                    
                //}
                snpData.CalculateVendorTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, "500");
                //snpData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, "500");                    
            }
        }


        private Control calendarCaller;

        //private void monthCalendar1_DateChanged(object sender, DateRangeEventArgs e)
        //{
        //    string selectedDate = monthCalendar1.SelectionEnd.ToShortDateString();
        //    if (calendarCaller == lnkStartDate)
        //    {
        //        lnkStartDate.Text = selectedDate;
        //        startDate = monthCalendar1.SelectionEnd;
        //        bStartDateSelected = true;
        //    }
        //    else if (calendarCaller == lnkEndDate)
        //    {
        //        lnkEndDate.Text = selectedDate;
        //        endDate = monthCalendar1.SelectionEnd;
        //        bEndDateSelected = true;
        //    }
        //    monthCalendar1.Hide();
        //}

        private void monthCalendar1_DateSelected(object sender, DateRangeEventArgs e)
        {
            string selectedDate = monthCalendar1.SelectionEnd.ToShortDateString();
            if (calendarCaller == lnkStartDate)
            {
                lnkStartDate.Text = selectedDate;
                startDate = monthCalendar1.SelectionEnd;
                bStartDateSelected = true;
            }
            else if (calendarCaller == lnkEndDate)
            {
                lnkEndDate.Text = selectedDate;
                endDate = monthCalendar1.SelectionEnd;
                bEndDateSelected = true;
            }
            monthCalendar1.Hide();
        }

        private void lnkStartDate_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            selectCalendarBizDate2(lnkStartDate);
        }

        private void lnkEndDate_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            selectCalendarBizDate2(lnkEndDate);
        }

        private void selectCalendarBizDate2(Control caller)
        {
            calendarCaller = caller;
            if (calendarCaller == lnkStartDate)
            {
                startDate = Convert.ToDateTime(lnkStartDate.Text);
                monthCalendar1.SetDate(startDate);
            }
            else if (calendarCaller == lnkEndDate)
            {
                endDate = Convert.ToDateTime(lnkEndDate.Text);
                monthCalendar1.SetDate(endDate);
            }
            monthCalendar1.Show();
        }
        #endregion

        private void cboVendor_SelectedIndexChanged(object sender, EventArgs e)
        {
            string[] Indices = null;

            cbRussellIndices.Items.Clear();

            if (cboVendor.SelectedItem.Equals("Russell"))
            {
                Indices = russellData.GetIndices();
                cbRussellIndices.Items.AddRange(Indices);
                cbRussellIndices.SelectedItem = "r3000";
            }
            else if (cboVendor.SelectedItem.Equals("Snp"))
            {
                Indices = snpData.GetIndices();
                cbRussellIndices.Items.AddRange(Indices);
                cbRussellIndices.SelectedItem = "sp500";
            }
        }

        private void btnTestAxmlOutput_Click(object sender, EventArgs e)
        {
            IndexDataQA indexDataQA = new IndexDataQA();
            string Indexname = cbRussellIndices.SelectedItem.ToString();
            string Vendor = cboVendor.SelectedItem.ToString();
            string OutputType = cboOutputType.SelectedItem.ToString();

            //indexDataQA.RunCompare();
            indexDataQA.CompareAxmlForDateRange(Vendor, OutputType, Indexname, lnkStartDate.Text, lnkEndDate.Text);
        }

        private void btnTestEndOfMonthDates_Click(object sender, EventArgs e)
        {
            snpData.TestEndOfMonthDates(lnkStartDate.Text, lnkEndDate.Text);

        }

        private void btnTestEmail_Click(object sender, EventArgs e)
        {
            //Mail mail = new Mail();
            //mail.SendMail("Message");

            //DateHelper.IsEndofMonthOnWeekend("06/28/2019");
            //DateHelper.IsEndofMonthOnWeekend("06/27/2019");

            //indexDataEngine = new IndexDataEngine();
            //indexDataEngine.CreateFtpFolders();
            //snpData.TestFileCopy();
            //snpData.TestFilesCopy();    

            // JK: I added this code to see how the new version compared to the old version in terms of code lines
            // old version has 173,234 lines of *.cs code
            // new version has 18,308   lines of *.cs code
            //string directory = @"C:\A_Development\visual studio 2017\Projects\Advent_ApxIndexData";
            //string directory = @"C:\A_Development\visual studio 2017\Projects\AdvIndexData";

            //string searchPattern = "*.cs";

            //if (directory == null || !System.IO.Directory.Exists(directory))
            //{
            //}
            //else
            //{ 
            //    var filesResults = System.IO.Directory.EnumerateFiles(directory, searchPattern, System.IO.SearchOption.AllDirectories)
            //                                .Select(file => new
            //                                {
            //                                    FilePath = file,
            //                                    TotalLines = System.IO.File.ReadLines(file).Count()
            //                                }).ToList();
            //    int totalLines = 0;
            //    foreach (var result in filesResults)
            //    {
            //        totalLines += result.TotalLines;
            //    }

            //    LogHelper.WriteLine("totalLines " + totalLines);
            //}


            indexDataEngine = new IndexDataEngine();
            //indexDataEngine.TestGetStatusSummary(lnkStartDate.Text);
            indexDataEngine.TestGenerateStatusReport(lnkStartDate.Text); 
        }

        private void btnTestFtp_Click(object sender, EventArgs e)
        {
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.InitializeHistoricalSecurityMasterCopy();

            //indexDataEngine.CreateFtpFolders();
            //snpData.TestFileCopy();
            //snpData.TestFilesCopy();
        }

        private void btnTestSecMaster_Click(object sender, EventArgs e)
        {
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.ProcessSecurityMasterChanges(lnkStartDate.Text);
        }

        private void btnTestSecMasterReport_Click(object sender, EventArgs e)
        {
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.ProcessSecurityMasterReport(lnkStartDate.Text);
        }

        private void btnProcessAllForDate_Click(object sender, EventArgs e)
        {
            IndexDataEngine indexDataEngine = new IndexDataEngine();
            indexDataEngine.Run(lnkStartDate.Text);
        }
    }
}
