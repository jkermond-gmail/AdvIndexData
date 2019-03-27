namespace IndexDataForm
{
    partial class IndexDataForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.btnStart = new System.Windows.Forms.Button();
            this.btnStop = new System.Windows.Forms.Button();
            this.timerRunIndexData = new System.Windows.Forms.Timer(this.components);
            this.tabUnitTest = new System.Windows.Forms.TabControl();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.chkHistoricalAxmlFile = new System.Windows.Forms.CheckBox();
            this.btnTestEndOfMonthDates = new System.Windows.Forms.Button();
            this.btnTestAxmlOutput = new System.Windows.Forms.Button();
            this.cboOutputType = new System.Windows.Forms.ComboBox();
            this.btnCalculateTotalReturns = new System.Windows.Forms.Button();
            this.btnUpdateRussellHoldings = new System.Windows.Forms.Button();
            this.btnGenerateReturns = new System.Windows.Forms.Button();
            this.btnCalculateAllIndexReturns = new System.Windows.Forms.Button();
            this.btnCalculateOneIndexReturns = new System.Windows.Forms.Button();
            this.cbRussellIndices = new System.Windows.Forms.ComboBox();
            this.monthCalendar1 = new System.Windows.Forms.MonthCalendar();
            this.lnkEndDate = new System.Windows.Forms.LinkLabel();
            this.lnkStartDate = new System.Windows.Forms.LinkLabel();
            this.lblEndDate = new System.Windows.Forms.Label();
            this.lblStartDate = new System.Windows.Forms.Label();
            this.cboVendor = new System.Windows.Forms.ComboBox();
            this.lblVendor = new System.Windows.Forms.Label();
            this.tabUnitTest.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnStart
            // 
            this.btnStart.Location = new System.Drawing.Point(13, 13);
            this.btnStart.Name = "btnStart";
            this.btnStart.Size = new System.Drawing.Size(75, 23);
            this.btnStart.TabIndex = 0;
            this.btnStart.Text = "Start";
            this.btnStart.UseVisualStyleBackColor = true;
            this.btnStart.Click += new System.EventHandler(this.btnStart_Click);
            // 
            // btnStop
            // 
            this.btnStop.Location = new System.Drawing.Point(13, 43);
            this.btnStop.Name = "btnStop";
            this.btnStop.Size = new System.Drawing.Size(75, 23);
            this.btnStop.TabIndex = 1;
            this.btnStop.Text = "Stop";
            this.btnStop.UseVisualStyleBackColor = true;
            this.btnStop.Click += new System.EventHandler(this.btnStop_Click);
            // 
            // timerRunIndexData
            // 
            this.timerRunIndexData.Tick += new System.EventHandler(this.timerRunIndexData_Tick);
            // 
            // tabUnitTest
            // 
            this.tabUnitTest.Controls.Add(this.tabPage1);
            this.tabUnitTest.Location = new System.Drawing.Point(13, 72);
            this.tabUnitTest.Name = "tabUnitTest";
            this.tabUnitTest.SelectedIndex = 0;
            this.tabUnitTest.Size = new System.Drawing.Size(604, 408);
            this.tabUnitTest.TabIndex = 2;
            // 
            // tabPage1
            // 
            this.tabPage1.Controls.Add(this.chkHistoricalAxmlFile);
            this.tabPage1.Controls.Add(this.btnTestEndOfMonthDates);
            this.tabPage1.Controls.Add(this.btnTestAxmlOutput);
            this.tabPage1.Controls.Add(this.cboOutputType);
            this.tabPage1.Controls.Add(this.btnCalculateTotalReturns);
            this.tabPage1.Controls.Add(this.btnUpdateRussellHoldings);
            this.tabPage1.Controls.Add(this.btnGenerateReturns);
            this.tabPage1.Controls.Add(this.btnCalculateAllIndexReturns);
            this.tabPage1.Controls.Add(this.btnCalculateOneIndexReturns);
            this.tabPage1.Controls.Add(this.cbRussellIndices);
            this.tabPage1.Controls.Add(this.monthCalendar1);
            this.tabPage1.Controls.Add(this.lnkEndDate);
            this.tabPage1.Controls.Add(this.lnkStartDate);
            this.tabPage1.Controls.Add(this.lblEndDate);
            this.tabPage1.Controls.Add(this.lblStartDate);
            this.tabPage1.Location = new System.Drawing.Point(4, 22);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage1.Size = new System.Drawing.Size(596, 382);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Text = "Russell";
            this.tabPage1.UseVisualStyleBackColor = true;
            this.tabPage1.Click += new System.EventHandler(this.tabPage1_Click);
            // 
            // chkHistoricalAxmlFile
            // 
            this.chkHistoricalAxmlFile.AutoSize = true;
            this.chkHistoricalAxmlFile.Location = new System.Drawing.Point(228, 305);
            this.chkHistoricalAxmlFile.Name = "chkHistoricalAxmlFile";
            this.chkHistoricalAxmlFile.Size = new System.Drawing.Size(113, 17);
            this.chkHistoricalAxmlFile.TabIndex = 14;
            this.chkHistoricalAxmlFile.Text = "Historical Axml File";
            this.chkHistoricalAxmlFile.UseVisualStyleBackColor = true;
            // 
            // btnTestEndOfMonthDates
            // 
            this.btnTestEndOfMonthDates.Location = new System.Drawing.Point(348, 329);
            this.btnTestEndOfMonthDates.Name = "btnTestEndOfMonthDates";
            this.btnTestEndOfMonthDates.Size = new System.Drawing.Size(121, 23);
            this.btnTestEndOfMonthDates.TabIndex = 13;
            this.btnTestEndOfMonthDates.Text = "Test EOM Dates";
            this.btnTestEndOfMonthDates.UseVisualStyleBackColor = true;
            this.btnTestEndOfMonthDates.Click += new System.EventHandler(this.btnTestEndOfMonthDates_Click);
            // 
            // btnTestAxmlOutput
            // 
            this.btnTestAxmlOutput.Location = new System.Drawing.Point(348, 287);
            this.btnTestAxmlOutput.Name = "btnTestAxmlOutput";
            this.btnTestAxmlOutput.Size = new System.Drawing.Size(121, 23);
            this.btnTestAxmlOutput.TabIndex = 12;
            this.btnTestAxmlOutput.Text = "TestAxmlOutput";
            this.btnTestAxmlOutput.UseVisualStyleBackColor = true;
            this.btnTestAxmlOutput.Click += new System.EventHandler(this.btnTestAxmlOutput_Click);
            // 
            // cboOutputType
            // 
            this.cboOutputType.FormattingEnabled = true;
            this.cboOutputType.Items.AddRange(new object[] {
            "Constituent",
            "Sector"});
            this.cboOutputType.Location = new System.Drawing.Point(256, 241);
            this.cboOutputType.Name = "cboOutputType";
            this.cboOutputType.Size = new System.Drawing.Size(121, 21);
            this.cboOutputType.TabIndex = 11;
            this.cboOutputType.Text = "Sector";
            // 
            // btnCalculateTotalReturns
            // 
            this.btnCalculateTotalReturns.Location = new System.Drawing.Point(39, 329);
            this.btnCalculateTotalReturns.Name = "btnCalculateTotalReturns";
            this.btnCalculateTotalReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateTotalReturns.TabIndex = 10;
            this.btnCalculateTotalReturns.Text = "Calculate All Total Returns";
            this.btnCalculateTotalReturns.UseVisualStyleBackColor = true;
            this.btnCalculateTotalReturns.Click += new System.EventHandler(this.btnCalculateTotalReturns_Click);
            // 
            // btnUpdateRussellHoldings
            // 
            this.btnUpdateRussellHoldings.Location = new System.Drawing.Point(39, 198);
            this.btnUpdateRussellHoldings.Name = "btnUpdateRussellHoldings";
            this.btnUpdateRussellHoldings.Size = new System.Drawing.Size(163, 23);
            this.btnUpdateRussellHoldings.TabIndex = 9;
            this.btnUpdateRussellHoldings.Text = "Update Holdings Db";
            this.btnUpdateRussellHoldings.UseVisualStyleBackColor = true;
            this.btnUpdateRussellHoldings.Click += new System.EventHandler(this.btnUpdateRussellHoldings_Click);
            // 
            // btnGenerateReturns
            // 
            this.btnGenerateReturns.Location = new System.Drawing.Point(39, 299);
            this.btnGenerateReturns.Name = "btnGenerateReturns";
            this.btnGenerateReturns.Size = new System.Drawing.Size(163, 23);
            this.btnGenerateReturns.TabIndex = 8;
            this.btnGenerateReturns.Text = "Generate Returns";
            this.btnGenerateReturns.UseVisualStyleBackColor = true;
            this.btnGenerateReturns.Click += new System.EventHandler(this.btnGenerateReturns_Click);
            // 
            // btnCalculateAllIndexReturns
            // 
            this.btnCalculateAllIndexReturns.Location = new System.Drawing.Point(39, 270);
            this.btnCalculateAllIndexReturns.Name = "btnCalculateAllIndexReturns";
            this.btnCalculateAllIndexReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateAllIndexReturns.TabIndex = 7;
            this.btnCalculateAllIndexReturns.Text = "Calculate All Index Returns";
            this.btnCalculateAllIndexReturns.UseVisualStyleBackColor = true;
            this.btnCalculateAllIndexReturns.Click += new System.EventHandler(this.btnCalculateAllIndexReturns_Click);
            // 
            // btnCalculateOneIndexReturns
            // 
            this.btnCalculateOneIndexReturns.Location = new System.Drawing.Point(39, 241);
            this.btnCalculateOneIndexReturns.Name = "btnCalculateOneIndexReturns";
            this.btnCalculateOneIndexReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateOneIndexReturns.TabIndex = 6;
            this.btnCalculateOneIndexReturns.Text = "Calculate One Index Returns";
            this.btnCalculateOneIndexReturns.UseVisualStyleBackColor = true;
            this.btnCalculateOneIndexReturns.Click += new System.EventHandler(this.btnCalculateOneIndexReturns_Click);
            // 
            // cbRussellIndices
            // 
            this.cbRussellIndices.FormattingEnabled = true;
            this.cbRussellIndices.Location = new System.Drawing.Point(39, 148);
            this.cbRussellIndices.Name = "cbRussellIndices";
            this.cbRussellIndices.Size = new System.Drawing.Size(121, 21);
            this.cbRussellIndices.TabIndex = 5;
            this.cbRussellIndices.Text = "sp500";
            // 
            // monthCalendar1
            // 
            this.monthCalendar1.Location = new System.Drawing.Point(256, 60);
            this.monthCalendar1.Name = "monthCalendar1";
            this.monthCalendar1.TabIndex = 4;
            this.monthCalendar1.DateSelected += new System.Windows.Forms.DateRangeEventHandler(this.monthCalendar1_DateSelected);
            // 
            // lnkEndDate
            // 
            this.lnkEndDate.AutoSize = true;
            this.lnkEndDate.Location = new System.Drawing.Point(140, 103);
            this.lnkEndDate.Name = "lnkEndDate";
            this.lnkEndDate.Size = new System.Drawing.Size(53, 13);
            this.lnkEndDate.TabIndex = 3;
            this.lnkEndDate.TabStop = true;
            this.lnkEndDate.Text = "04/08/15";
            this.lnkEndDate.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.lnkEndDate_LinkClicked);
            // 
            // lnkStartDate
            // 
            this.lnkStartDate.AutoSize = true;
            this.lnkStartDate.Location = new System.Drawing.Point(137, 60);
            this.lnkStartDate.Name = "lnkStartDate";
            this.lnkStartDate.Size = new System.Drawing.Size(53, 13);
            this.lnkStartDate.TabIndex = 2;
            this.lnkStartDate.TabStop = true;
            this.lnkStartDate.Text = "04/08/15";
            this.lnkStartDate.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.lnkStartDate_LinkClicked);
            // 
            // lblEndDate
            // 
            this.lblEndDate.AutoSize = true;
            this.lblEndDate.Location = new System.Drawing.Point(36, 104);
            this.lblEndDate.Name = "lblEndDate";
            this.lblEndDate.Size = new System.Drawing.Size(52, 13);
            this.lblEndDate.TabIndex = 1;
            this.lblEndDate.Text = "End Date";
            // 
            // lblStartDate
            // 
            this.lblStartDate.AutoSize = true;
            this.lblStartDate.Location = new System.Drawing.Point(36, 60);
            this.lblStartDate.Name = "lblStartDate";
            this.lblStartDate.Size = new System.Drawing.Size(55, 13);
            this.lblStartDate.TabIndex = 0;
            this.lblStartDate.Text = "Start Date";
            // 
            // cboVendor
            // 
            this.cboVendor.FormattingEnabled = true;
            this.cboVendor.Items.AddRange(new object[] {
            "Russell",
            "Snp"});
            this.cboVendor.Location = new System.Drawing.Point(204, 15);
            this.cboVendor.Name = "cboVendor";
            this.cboVendor.Size = new System.Drawing.Size(121, 21);
            this.cboVendor.TabIndex = 3;
            this.cboVendor.Text = "Snp";
            this.cboVendor.SelectedIndexChanged += new System.EventHandler(this.cboVendor_SelectedIndexChanged);
            // 
            // lblVendor
            // 
            this.lblVendor.AutoSize = true;
            this.lblVendor.Location = new System.Drawing.Point(157, 18);
            this.lblVendor.Name = "lblVendor";
            this.lblVendor.Size = new System.Drawing.Size(41, 13);
            this.lblVendor.TabIndex = 4;
            this.lblVendor.Text = "Vendor";
            // 
            // IndexDataForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(640, 492);
            this.Controls.Add(this.lblVendor);
            this.Controls.Add(this.cboVendor);
            this.Controls.Add(this.tabUnitTest);
            this.Controls.Add(this.btnStop);
            this.Controls.Add(this.btnStart);
            this.Name = "IndexDataForm";
            this.Text = "Advent Index Data";
            this.tabUnitTest.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnStart;
        private System.Windows.Forms.Button btnStop;
        private System.Windows.Forms.Timer timerRunIndexData;
        private System.Windows.Forms.TabControl tabUnitTest;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.ComboBox cbRussellIndices;
        private System.Windows.Forms.MonthCalendar monthCalendar1;
        private System.Windows.Forms.LinkLabel lnkEndDate;
        private System.Windows.Forms.LinkLabel lnkStartDate;
        private System.Windows.Forms.Label lblEndDate;
        private System.Windows.Forms.Label lblStartDate;
        private System.Windows.Forms.Button btnCalculateAllIndexReturns;
        private System.Windows.Forms.Button btnCalculateOneIndexReturns;
        private System.Windows.Forms.Button btnGenerateReturns;
        private System.Windows.Forms.Button btnUpdateRussellHoldings;
        private System.Windows.Forms.Button btnCalculateTotalReturns;
        private System.Windows.Forms.ComboBox cboVendor;
        private System.Windows.Forms.Label lblVendor;
        private System.Windows.Forms.ComboBox cboOutputType;
        private System.Windows.Forms.Button btnTestAxmlOutput;
        private System.Windows.Forms.Button btnTestEndOfMonthDates;
        private System.Windows.Forms.CheckBox chkHistoricalAxmlFile;
    }
}

