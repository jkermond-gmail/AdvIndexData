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
            this.cboVendor = new System.Windows.Forms.ComboBox();
            this.lblVendor = new System.Windows.Forms.Label();
            this.chkLogReturnData = new System.Windows.Forms.CheckBox();
            this.btnTestSecMasterReport = new System.Windows.Forms.Button();
            this.btnTestSecMaster = new System.Windows.Forms.Button();
            this.btnProcessAllForDate = new System.Windows.Forms.Button();
            this.btnTestFtp = new System.Windows.Forms.Button();
            this.btnTestEmail = new System.Windows.Forms.Button();
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
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.lblFileDate2 = new System.Windows.Forms.Label();
            this.lblIndex = new System.Windows.Forms.Label();
            this.btnCopyFiles = new System.Windows.Forms.Button();
            this.txtFileDate = new System.Windows.Forms.TextBox();
            this.lblFileDate = new System.Windows.Forms.Label();
            this.monthCalendar2 = new System.Windows.Forms.MonthCalendar();
            this.cboIndices2 = new System.Windows.Forms.ComboBox();
            this.lblOutputType = new System.Windows.Forms.Label();
            this.cboOutputType2 = new System.Windows.Forms.ComboBox();
            this.cboClients = new System.Windows.Forms.ComboBox();
            this.lblClients = new System.Windows.Forms.Label();
            this.tabUnitTest.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnStart
            // 
            this.btnStart.Location = new System.Drawing.Point(13, 13);
            this.btnStart.Name = "btnStart";
            this.btnStart.Size = new System.Drawing.Size(147, 23);
            this.btnStart.TabIndex = 0;
            this.btnStart.Text = "Start Index Data Engine";
            this.btnStart.UseVisualStyleBackColor = true;
            this.btnStart.Click += new System.EventHandler(this.btnStart_Click);
            // 
            // btnStop
            // 
            this.btnStop.Location = new System.Drawing.Point(13, 43);
            this.btnStop.Name = "btnStop";
            this.btnStop.Size = new System.Drawing.Size(147, 23);
            this.btnStop.TabIndex = 1;
            this.btnStop.Text = "Stop Index Data Engine";
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
            this.tabUnitTest.Controls.Add(this.tabPage2);
            this.tabUnitTest.Location = new System.Drawing.Point(13, 72);
            this.tabUnitTest.Name = "tabUnitTest";
            this.tabUnitTest.SelectedIndex = 0;
            this.tabUnitTest.Size = new System.Drawing.Size(451, 517);
            this.tabUnitTest.TabIndex = 2;
            // 
            // tabPage1
            // 
            this.tabPage1.Controls.Add(this.cboVendor);
            this.tabPage1.Controls.Add(this.lblVendor);
            this.tabPage1.Controls.Add(this.chkLogReturnData);
            this.tabPage1.Controls.Add(this.btnTestSecMasterReport);
            this.tabPage1.Controls.Add(this.btnTestSecMaster);
            this.tabPage1.Controls.Add(this.btnProcessAllForDate);
            this.tabPage1.Controls.Add(this.btnTestFtp);
            this.tabPage1.Controls.Add(this.btnTestEmail);
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
            this.tabPage1.Size = new System.Drawing.Size(443, 491);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Text = "Index Data Utilities";
            this.tabPage1.UseVisualStyleBackColor = true;
            // 
            // cboVendor
            // 
            this.cboVendor.FormattingEnabled = true;
            this.cboVendor.Items.AddRange(new object[] {
            "RussellIcb",
            "Snp",
            "Russell",
            "RussellIcbHist"});
            this.cboVendor.Location = new System.Drawing.Point(48, 248);
            this.cboVendor.Name = "cboVendor";
            this.cboVendor.Size = new System.Drawing.Size(121, 21);
            this.cboVendor.TabIndex = 3;
            this.cboVendor.Text = "Snp";
            this.cboVendor.SelectedIndexChanged += new System.EventHandler(this.cboVendor_SelectedIndexChanged);
            // 
            // lblVendor
            // 
            this.lblVendor.AutoSize = true;
            this.lblVendor.Location = new System.Drawing.Point(6, 251);
            this.lblVendor.Name = "lblVendor";
            this.lblVendor.Size = new System.Drawing.Size(41, 13);
            this.lblVendor.TabIndex = 4;
            this.lblVendor.Text = "Vendor";
            // 
            // chkLogReturnData
            // 
            this.chkLogReturnData.AutoSize = true;
            this.chkLogReturnData.Location = new System.Drawing.Point(150, 341);
            this.chkLogReturnData.Name = "chkLogReturnData";
            this.chkLogReturnData.Size = new System.Drawing.Size(105, 17);
            this.chkLogReturnData.TabIndex = 20;
            this.chkLogReturnData.Text = "Log Return Data";
            this.chkLogReturnData.UseVisualStyleBackColor = true;
            // 
            // btnTestSecMasterReport
            // 
            this.btnTestSecMasterReport.Location = new System.Drawing.Point(302, 426);
            this.btnTestSecMasterReport.Name = "btnTestSecMasterReport";
            this.btnTestSecMasterReport.Size = new System.Drawing.Size(139, 23);
            this.btnTestSecMasterReport.TabIndex = 19;
            this.btnTestSecMasterReport.Text = "Test Sec Master Report";
            this.btnTestSecMasterReport.UseVisualStyleBackColor = true;
            this.btnTestSecMasterReport.Visible = false;
            this.btnTestSecMasterReport.Click += new System.EventHandler(this.btnTestSecMasterReport_Click);
            // 
            // btnTestSecMaster
            // 
            this.btnTestSecMaster.Location = new System.Drawing.Point(165, 426);
            this.btnTestSecMaster.Name = "btnTestSecMaster";
            this.btnTestSecMaster.Size = new System.Drawing.Size(139, 23);
            this.btnTestSecMaster.TabIndex = 18;
            this.btnTestSecMaster.Text = "Test Sec Master Changes";
            this.btnTestSecMaster.UseVisualStyleBackColor = true;
            this.btnTestSecMaster.Click += new System.EventHandler(this.btnTestSecMaster_Click);
            // 
            // btnProcessAllForDate
            // 
            this.btnProcessAllForDate.Location = new System.Drawing.Point(9, 219);
            this.btnProcessAllForDate.Name = "btnProcessAllForDate";
            this.btnProcessAllForDate.Size = new System.Drawing.Size(384, 23);
            this.btnProcessAllForDate.TabIndex = 17;
            this.btnProcessAllForDate.Text = "Process Vendor Files, Generate AXML Files, Distribute to FTP for Date Range";
            this.btnProcessAllForDate.UseVisualStyleBackColor = true;
            this.btnProcessAllForDate.Click += new System.EventHandler(this.btnProcessAllForDate_Click);
            // 
            // btnTestFtp
            // 
            this.btnTestFtp.Location = new System.Drawing.Point(165, 397);
            this.btnTestFtp.Name = "btnTestFtp";
            this.btnTestFtp.Size = new System.Drawing.Size(139, 23);
            this.btnTestFtp.TabIndex = 16;
            this.btnTestFtp.Text = "Test Sec Master Init";
            this.btnTestFtp.UseVisualStyleBackColor = true;
            this.btnTestFtp.Visible = false;
            this.btnTestFtp.Click += new System.EventHandler(this.btnTestFtp_Click);
            // 
            // btnTestEmail
            // 
            this.btnTestEmail.Location = new System.Drawing.Point(9, 187);
            this.btnTestEmail.Name = "btnTestEmail";
            this.btnTestEmail.Size = new System.Drawing.Size(384, 23);
            this.btnTestEmail.TabIndex = 15;
            this.btnTestEmail.Text = "Generate and Email Status Report for Start Date";
            this.btnTestEmail.UseVisualStyleBackColor = true;
            this.btnTestEmail.Click += new System.EventHandler(this.btnTestEmail_Click);
            // 
            // chkHistoricalAxmlFile
            // 
            this.chkHistoricalAxmlFile.AutoSize = true;
            this.chkHistoricalAxmlFile.Location = new System.Drawing.Point(150, 314);
            this.chkHistoricalAxmlFile.Name = "chkHistoricalAxmlFile";
            this.chkHistoricalAxmlFile.Size = new System.Drawing.Size(113, 17);
            this.chkHistoricalAxmlFile.TabIndex = 14;
            this.chkHistoricalAxmlFile.Text = "Historical Axml File";
            this.chkHistoricalAxmlFile.UseVisualStyleBackColor = true;
            // 
            // btnTestEndOfMonthDates
            // 
            this.btnTestEndOfMonthDates.Location = new System.Drawing.Point(302, 397);
            this.btnTestEndOfMonthDates.Name = "btnTestEndOfMonthDates";
            this.btnTestEndOfMonthDates.Size = new System.Drawing.Size(121, 23);
            this.btnTestEndOfMonthDates.TabIndex = 13;
            this.btnTestEndOfMonthDates.Text = "Test EOM Dates";
            this.btnTestEndOfMonthDates.UseVisualStyleBackColor = true;
            this.btnTestEndOfMonthDates.Visible = false;
            this.btnTestEndOfMonthDates.Click += new System.EventHandler(this.btnTestEndOfMonthDates_Click);
            // 
            // btnTestAxmlOutput
            // 
            this.btnTestAxmlOutput.Location = new System.Drawing.Point(6, 455);
            this.btnTestAxmlOutput.Name = "btnTestAxmlOutput";
            this.btnTestAxmlOutput.Size = new System.Drawing.Size(121, 23);
            this.btnTestAxmlOutput.TabIndex = 12;
            this.btnTestAxmlOutput.Text = "TestAxmlOutput";
            this.btnTestAxmlOutput.UseVisualStyleBackColor = true;
            this.btnTestAxmlOutput.Visible = false;
            this.btnTestAxmlOutput.Click += new System.EventHandler(this.btnTestAxmlOutput_Click);
            // 
            // cboOutputType
            // 
            this.cboOutputType.FormattingEnabled = true;
            this.cboOutputType.Items.AddRange(new object[] {
            "Constituent",
            "Sector"});
            this.cboOutputType.Location = new System.Drawing.Point(9, 341);
            this.cboOutputType.Name = "cboOutputType";
            this.cboOutputType.Size = new System.Drawing.Size(121, 21);
            this.cboOutputType.TabIndex = 11;
            this.cboOutputType.Text = "Sector";
            // 
            // btnCalculateTotalReturns
            // 
            this.btnCalculateTotalReturns.Location = new System.Drawing.Point(133, 455);
            this.btnCalculateTotalReturns.Name = "btnCalculateTotalReturns";
            this.btnCalculateTotalReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateTotalReturns.TabIndex = 10;
            this.btnCalculateTotalReturns.Text = "Calculate All Total Returns";
            this.btnCalculateTotalReturns.UseVisualStyleBackColor = true;
            this.btnCalculateTotalReturns.Visible = false;
            this.btnCalculateTotalReturns.Click += new System.EventHandler(this.btnCalculateTotalReturns_Click);
            // 
            // btnUpdateRussellHoldings
            // 
            this.btnUpdateRussellHoldings.Location = new System.Drawing.Point(9, 272);
            this.btnUpdateRussellHoldings.Name = "btnUpdateRussellHoldings";
            this.btnUpdateRussellHoldings.Size = new System.Drawing.Size(384, 23);
            this.btnUpdateRussellHoldings.TabIndex = 9;
            this.btnUpdateRussellHoldings.Text = "Process Vendor Files for Date Range";
            this.btnUpdateRussellHoldings.UseVisualStyleBackColor = true;
            this.btnUpdateRussellHoldings.Click += new System.EventHandler(this.btnUpdateRussellHoldings_Click);
            // 
            // btnGenerateReturns
            // 
            this.btnGenerateReturns.Location = new System.Drawing.Point(9, 368);
            this.btnGenerateReturns.Name = "btnGenerateReturns";
            this.btnGenerateReturns.Size = new System.Drawing.Size(387, 23);
            this.btnGenerateReturns.TabIndex = 8;
            this.btnGenerateReturns.Text = "Generate Returns for Date Range";
            this.btnGenerateReturns.UseVisualStyleBackColor = true;
            this.btnGenerateReturns.Click += new System.EventHandler(this.btnGenerateReturns_Click);
            // 
            // btnCalculateAllIndexReturns
            // 
            this.btnCalculateAllIndexReturns.Location = new System.Drawing.Point(6, 426);
            this.btnCalculateAllIndexReturns.Name = "btnCalculateAllIndexReturns";
            this.btnCalculateAllIndexReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateAllIndexReturns.TabIndex = 7;
            this.btnCalculateAllIndexReturns.Text = "Calculate All Index Returns";
            this.btnCalculateAllIndexReturns.UseVisualStyleBackColor = true;
            this.btnCalculateAllIndexReturns.Visible = false;
            this.btnCalculateAllIndexReturns.Click += new System.EventHandler(this.btnCalculateAllIndexReturns_Click);
            // 
            // btnCalculateOneIndexReturns
            // 
            this.btnCalculateOneIndexReturns.Location = new System.Drawing.Point(6, 397);
            this.btnCalculateOneIndexReturns.Name = "btnCalculateOneIndexReturns";
            this.btnCalculateOneIndexReturns.Size = new System.Drawing.Size(163, 23);
            this.btnCalculateOneIndexReturns.TabIndex = 6;
            this.btnCalculateOneIndexReturns.Text = "Calculate One Index Returns";
            this.btnCalculateOneIndexReturns.UseVisualStyleBackColor = true;
            this.btnCalculateOneIndexReturns.Visible = false;
            this.btnCalculateOneIndexReturns.Click += new System.EventHandler(this.btnCalculateOneIndexReturns_Click);
            // 
            // cbRussellIndices
            // 
            this.cbRussellIndices.FormattingEnabled = true;
            this.cbRussellIndices.Location = new System.Drawing.Point(9, 314);
            this.cbRussellIndices.Name = "cbRussellIndices";
            this.cbRussellIndices.Size = new System.Drawing.Size(121, 21);
            this.cbRussellIndices.TabIndex = 5;
            this.cbRussellIndices.Text = "sp500";
            // 
            // monthCalendar1
            // 
            this.monthCalendar1.Location = new System.Drawing.Point(194, 12);
            this.monthCalendar1.Name = "monthCalendar1";
            this.monthCalendar1.TabIndex = 4;
            this.monthCalendar1.DateSelected += new System.Windows.Forms.DateRangeEventHandler(this.monthCalendar1_DateSelected);
            // 
            // lnkEndDate
            // 
            this.lnkEndDate.AutoSize = true;
            this.lnkEndDate.Location = new System.Drawing.Point(62, 34);
            this.lnkEndDate.Name = "lnkEndDate";
            this.lnkEndDate.Size = new System.Drawing.Size(65, 13);
            this.lnkEndDate.TabIndex = 3;
            this.lnkEndDate.TabStop = true;
            this.lnkEndDate.Text = "12/31/2020";
            this.lnkEndDate.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.lnkEndDate_LinkClicked);
            // 
            // lnkStartDate
            // 
            this.lnkStartDate.AutoSize = true;
            this.lnkStartDate.Location = new System.Drawing.Point(62, 12);
            this.lnkStartDate.Name = "lnkStartDate";
            this.lnkStartDate.Size = new System.Drawing.Size(65, 13);
            this.lnkStartDate.TabIndex = 2;
            this.lnkStartDate.TabStop = true;
            this.lnkStartDate.Text = "12/22/2020";
            this.lnkStartDate.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.lnkStartDate_LinkClicked);
            // 
            // lblEndDate
            // 
            this.lblEndDate.AutoSize = true;
            this.lblEndDate.Location = new System.Drawing.Point(6, 34);
            this.lblEndDate.Name = "lblEndDate";
            this.lblEndDate.Size = new System.Drawing.Size(52, 13);
            this.lblEndDate.TabIndex = 1;
            this.lblEndDate.Text = "End Date";
            // 
            // lblStartDate
            // 
            this.lblStartDate.AutoSize = true;
            this.lblStartDate.Location = new System.Drawing.Point(6, 12);
            this.lblStartDate.Name = "lblStartDate";
            this.lblStartDate.Size = new System.Drawing.Size(55, 13);
            this.lblStartDate.TabIndex = 0;
            this.lblStartDate.Text = "Start Date";
            // 
            // tabPage2
            // 
            this.tabPage2.Controls.Add(this.lblFileDate2);
            this.tabPage2.Controls.Add(this.lblIndex);
            this.tabPage2.Controls.Add(this.btnCopyFiles);
            this.tabPage2.Controls.Add(this.txtFileDate);
            this.tabPage2.Controls.Add(this.lblFileDate);
            this.tabPage2.Controls.Add(this.monthCalendar2);
            this.tabPage2.Controls.Add(this.cboIndices2);
            this.tabPage2.Controls.Add(this.lblOutputType);
            this.tabPage2.Controls.Add(this.cboOutputType2);
            this.tabPage2.Controls.Add(this.cboClients);
            this.tabPage2.Controls.Add(this.lblClients);
            this.tabPage2.Location = new System.Drawing.Point(4, 22);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage2.Size = new System.Drawing.Size(443, 491);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Text = "Copy Client Files";
            this.tabPage2.UseVisualStyleBackColor = true;
            // 
            // lblFileDate2
            // 
            this.lblFileDate2.AutoSize = true;
            this.lblFileDate2.Location = new System.Drawing.Point(23, 110);
            this.lblFileDate2.Name = "lblFileDate2";
            this.lblFileDate2.Size = new System.Drawing.Size(82, 13);
            this.lblFileDate2.TabIndex = 20;
            this.lblFileDate2.Text = "Select File Date";
            // 
            // lblIndex
            // 
            this.lblIndex.AutoSize = true;
            this.lblIndex.Location = new System.Drawing.Point(39, 50);
            this.lblIndex.Name = "lblIndex";
            this.lblIndex.Size = new System.Drawing.Size(66, 13);
            this.lblIndex.TabIndex = 19;
            this.lblIndex.Text = "Select Index";
            // 
            // btnCopyFiles
            // 
            this.btnCopyFiles.Location = new System.Drawing.Point(111, 337);
            this.btnCopyFiles.Name = "btnCopyFiles";
            this.btnCopyFiles.Size = new System.Drawing.Size(199, 23);
            this.btnCopyFiles.TabIndex = 18;
            this.btnCopyFiles.Text = "Copy Files to Client\'s FTP Folder";
            this.btnCopyFiles.UseVisualStyleBackColor = true;
            this.btnCopyFiles.Click += new System.EventHandler(this.btnCopyFiles_Click);
            // 
            // txtFileDate
            // 
            this.txtFileDate.Location = new System.Drawing.Point(111, 292);
            this.txtFileDate.Name = "txtFileDate";
            this.txtFileDate.Size = new System.Drawing.Size(100, 20);
            this.txtFileDate.TabIndex = 17;
            // 
            // lblFileDate
            // 
            this.lblFileDate.AutoSize = true;
            this.lblFileDate.Location = new System.Drawing.Point(56, 295);
            this.lblFileDate.Name = "lblFileDate";
            this.lblFileDate.Size = new System.Drawing.Size(49, 13);
            this.lblFileDate.TabIndex = 16;
            this.lblFileDate.Text = "File Date";
            // 
            // monthCalendar2
            // 
            this.monthCalendar2.Location = new System.Drawing.Point(111, 110);
            this.monthCalendar2.Name = "monthCalendar2";
            this.monthCalendar2.TabIndex = 15;
            this.monthCalendar2.DateChanged += new System.Windows.Forms.DateRangeEventHandler(this.monthCalendar2_DateChanged);
            // 
            // cboIndices2
            // 
            this.cboIndices2.FormattingEnabled = true;
            this.cboIndices2.Location = new System.Drawing.Point(111, 47);
            this.cboIndices2.Name = "cboIndices2";
            this.cboIndices2.Size = new System.Drawing.Size(121, 21);
            this.cboIndices2.TabIndex = 14;
            this.cboIndices2.SelectedIndexChanged += new System.EventHandler(this.cboIndices2_SelectedIndexChanged);
            // 
            // lblOutputType
            // 
            this.lblOutputType.AutoSize = true;
            this.lblOutputType.Location = new System.Drawing.Point(6, 80);
            this.lblOutputType.Name = "lblOutputType";
            this.lblOutputType.Size = new System.Drawing.Size(99, 13);
            this.lblOutputType.TabIndex = 13;
            this.lblOutputType.Text = "Select Output Type";
            // 
            // cboOutputType2
            // 
            this.cboOutputType2.FormattingEnabled = true;
            this.cboOutputType2.Location = new System.Drawing.Point(111, 77);
            this.cboOutputType2.Name = "cboOutputType2";
            this.cboOutputType2.Size = new System.Drawing.Size(121, 21);
            this.cboOutputType2.TabIndex = 12;
            this.cboOutputType2.SelectedIndexChanged += new System.EventHandler(this.cboOutputType2_SelectedIndexChanged);
            // 
            // cboClients
            // 
            this.cboClients.FormattingEnabled = true;
            this.cboClients.Location = new System.Drawing.Point(111, 17);
            this.cboClients.Name = "cboClients";
            this.cboClients.Size = new System.Drawing.Size(121, 21);
            this.cboClients.TabIndex = 1;
            this.cboClients.SelectedIndexChanged += new System.EventHandler(this.cbClients_SelectedIndexChanged);
            // 
            // lblClients
            // 
            this.lblClients.AutoSize = true;
            this.lblClients.Location = new System.Drawing.Point(39, 20);
            this.lblClients.Name = "lblClients";
            this.lblClients.Size = new System.Drawing.Size(66, 13);
            this.lblClients.TabIndex = 0;
            this.lblClients.Text = "Select Client";
            // 
            // IndexDataForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(470, 601);
            this.Controls.Add(this.tabUnitTest);
            this.Controls.Add(this.btnStop);
            this.Controls.Add(this.btnStart);
            this.Name = "IndexDataForm";
            this.Text = "Advent Index Data";
            this.tabUnitTest.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage1.PerformLayout();
            this.tabPage2.ResumeLayout(false);
            this.tabPage2.PerformLayout();
            this.ResumeLayout(false);

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
        private System.Windows.Forms.Button btnTestEmail;
        private System.Windows.Forms.Button btnTestFtp;
        private System.Windows.Forms.Button btnProcessAllForDate;
        private System.Windows.Forms.Button btnTestSecMaster;
        private System.Windows.Forms.Button btnTestSecMasterReport;
        private System.Windows.Forms.CheckBox chkLogReturnData;
        private System.Windows.Forms.TabPage tabPage2;
        private System.Windows.Forms.ComboBox cboClients;
        private System.Windows.Forms.Label lblClients;
        private System.Windows.Forms.ComboBox cboIndices2;
        private System.Windows.Forms.Label lblOutputType;
        private System.Windows.Forms.ComboBox cboOutputType2;
        private System.Windows.Forms.MonthCalendar monthCalendar2;
        private System.Windows.Forms.Label lblFileDate;
        private System.Windows.Forms.TextBox txtFileDate;
        private System.Windows.Forms.Button btnCopyFiles;
        private System.Windows.Forms.Label lblIndex;
        private System.Windows.Forms.Label lblFileDate2;
    }
}

