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
        internal int TIMER_INTERVAL = 1000 * 2;                         // 2 seconds
        //internal int TIMER_INTERVAL = 1000 * 20;                         // 20 seconds
        //internal int TIMER_INTERVAL = 60 * 1000 * 2;                         // 2 min
        //internal int TIMER_HOLD_WHILE_WORKING_INTERVAL = 60 * 1000 * 200 ;   // 200 min

        //private System.Timers.Timer timer1 = new System.Timers.Timer();

        private IndexDataEngine indexDataEngine;
        private RussellData russellData = null;
        private SnpData snpData = null;
        //private SharedData 


        public IndexDataForm()
        {
            InitializeComponent();
            bool deleteExisting = false;
            LogHelper.StartLog("AdvIndexDataLog.txt", @"C:\A_Development\visual studio 2017\Projects\AdvIndexData\IndexDataForm\Output\", deleteExisting);

            russellData = new RussellData();
            snpData = new SnpData();

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
            LogHelper.Info("timerRunIndexData_Tick", "IndexDataForm");
            TimerEnableDisable("Start Working", false);

            // Begin checking if there is any index data work to do
            indexDataEngine = new IndexDataEngine();
            indexDataEngine.Run();

            // End checking if there is any index data work to do

            TimerEnableDisable("Stop Working", true);

        }

        private void TimerEnableDisable(string message, bool enable)
        {

            LogHelper.Info(message, "IndexDataForm");

            if (enable)
            {
                timerRunIndexData.Interval = TIMER_INTERVAL;
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
                //Indexname = Indexname.ToUpper();

                if (cboOutputType.Text.Equals("Constituent"))
                    russellData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Constituent);
                else if (cboOutputType.Text.Equals("Sector"))
                    russellData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Sector);
            }
            else if (cboVendor.SelectedItem.Equals("S&P"))
            {
                if (cboOutputType.Text.Equals("Constituent"))
                    snpData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Constituent);
                else if (cboOutputType.Text.Equals("Sector"))
                    snpData.GenerateReturnsForDateRange(lnkStartDate.Text, lnkEndDate.Text, Indexname, AdventOutputType.Sector);
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
            else if (cboVendor.SelectedItem.Equals("S&P"))
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
            else if (cboVendor.SelectedItem.Equals("S&P"))
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

            if (cboVendor.SelectedItem.Equals("Russell"))
            {
                Indices = russellData.GetIndices();
                cbRussellIndices.Items.AddRange(Indices);
                cbRussellIndices.SelectedItem = "r3000";                
            }
            else if (cboVendor.SelectedItem.Equals("S&P"))
            {
                Indices = snpData.GetIndices();
                cbRussellIndices.Items.AddRange(Indices);
                cbRussellIndices.SelectedItem = "sp500";
            }
        }
    }
}
