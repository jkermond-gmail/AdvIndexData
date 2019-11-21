using System;
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
        #region TimerAndInitializationFunctionality

        internal int defaultTimerInterval = 1000 * 2;                           // 2 seconds
        internal int timerInterval = 1000 * 2;                                  // 2 seconds
        //internal int defaultTimerInterval = 1000 * 20;                        // 20 seconds
        //internal int defaultTimerInterval = 60 * 1000 * 2;                    // 2 min

        private IndexDataEngine indexDataEngine;
        private RussellData russellData = null;
        private SnpData snpData = null;

        public IndexDataForm()
        {
            InitializeComponent();
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
        #endregion TimerAndInitializationFunctionality

        #region CalendarAndDateFunctionality

        private DateTime startDate;
        private DateTime endDate;
        private bool bStartDateSelected = false;
        private bool bEndDateSelected = false;
        private Control calendarCaller;

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
        #endregion CalendarAndDateFunctionality

        #region VendorSelectionFunctionality
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
        #endregion VendorSelectionFunctionality

        #region GenerateReturnsFunctionality

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
        }
        #endregion GenerateReturnsFunctionality

        #region UpdateHoldingsFunctionality
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
        #endregion UpdateHoldingsFunctionality

        #region StatusReportFunctionality
        private void btnTestEmail_Click(object sender, EventArgs e)
        {
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.TestGenerateStatusReport(lnkStartDate.Text); 
        }
        #endregion StatusReportFunctionality

        #region RunIndexDataFunctionality
        private void btnProcessAllForDate_Click(object sender, EventArgs e)
        {
            IndexDataEngine indexDataEngine = new IndexDataEngine();
            indexDataEngine.Run(lnkStartDate.Text);
        }
        #endregion RunIndexDataFunctionality

        #region testUI_tFunctionalityNotVisble
        // ---------------------------------------------------------------------------
        // ---------------------------------------------------------------------------
        // -- All of the routines below are linked to test UI buttons that are set to Not Visible
        private void btnCalculateOneIndexReturns_Click(object sender, EventArgs e)
        {
            russellData.CalculateAdventTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, (string)cbRussellIndices.SelectedItem);
        }

        private void btnTestFtp_Click(object sender, EventArgs e)
        {
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.InitializeHistoricalSecurityMasterCopy();

            //indexDataEngine.CreateFtpFolders();
            //snpData.TestFileCopy();
            //snpData.TestFilesCopy();
        }

        private void btnTestEndOfMonthDates_Click(object sender, EventArgs e)
        {
            snpData.TestEndOfMonthDates(lnkStartDate.Text, lnkEndDate.Text);
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
                snpData.CalculateVendorTotalReturnsForPeriod(lnkStartDate.Text, lnkEndDate.Text, "500");
            }
        }

        private void btnTestAxmlOutput_Click(object sender, EventArgs e)
        {
            IndexDataQA indexDataQA = new IndexDataQA();
            string Indexname = cbRussellIndices.SelectedItem.ToString();
            string Vendor = cboVendor.SelectedItem.ToString();
            string OutputType = cboOutputType.SelectedItem.ToString();
            indexDataQA.CompareAxmlForDateRange(Vendor, OutputType, Indexname, lnkStartDate.Text, lnkEndDate.Text);
        }

        #endregion testUI_tFunctionalityNotVisble

    }
}
