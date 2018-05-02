using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

namespace IndexDataEngineLibrary
{
    internal sealed class IndexRow
    {

        private double mWeight; 
        private bool mWeightIsMktValue;
        private double mRateOfReturn; 
        private double mRateOfReturnAdjustment;
        private double mRateOfReturnAdjusted;
        private static double mTotalReturn = 0;
        private static double mTotalReturnAdjusted = 0;
        public enum VendorFormat
        {
            CONSTITUENT,
            SECTOR_LEVEL1,
            SECTOR_LEVEL2,
            SECTOR_LEVEL3,
            SECTOR_LEVEL4
        }
        private static uint mConstituentCount;
        private static uint mSectorLevel1Count;
        private static uint mSectorLevel2Count;
        private static uint mSectorLevel3Count;
        private static uint mSectorLevel4Count;

        private static VendorFormat mVendorFormat;

        /*
        mIndexDate = sIndexDate;
            mIndexname = sIndexname;
            mCUSIP = sCUSIP;
            mTicker = sTicker;
            mSectorLevel1 = sSector;
            mWeight = sWeight;
            mSecurityReturn = sSecurityReturn;
            */


        private DateTime mIndexDate;
        private string mIndexname;
        private string mCUSIP;
        private string mTicker;
        private string mSectorLevel1;
        private string mSectorLevel2;
        private string mSectorLevel3;
        private string mSectorLevel4;

        private string mIdentifier;

        private static double mAdventVsVendorDiff;
        private static double mAddlContribution;

        private CultureInfo mCultureInfo = new CultureInfo("en-US");


        internal double AdventVsVendorDiff
        {
            get { return mAdventVsVendorDiff; }
            set { mAdventVsVendorDiff = value; }
        }


        internal double RateOfReturn
        {
            get { return mRateOfReturn; }
            set { mRateOfReturn = value; }
        }
        
        internal double RateOfReturnAdjustment
        {
            get { return mRateOfReturnAdjustment;}
            set { mRateOfReturnAdjustment = value; }
        }

        internal double RateOfReturnAdjusted
        {
            get { return mRateOfReturnAdjusted; }
            set { mRateOfReturnAdjusted = value; }
        }


        internal string Ticker
        {
            get { return mTicker; }
        }

        internal string CUSIP
        {
            get { return mCUSIP; }
        }

        internal string SectorLevel1
        {
            get { return mSectorLevel1; }
        }

        internal string SectorLevel2
        {
            get { return mSectorLevel2; }
        }

        internal string SectorLevel3
        {
            get { return mSectorLevel3; }
        }

        internal string SectorLevel4
        {
            get { return mSectorLevel4; }
        }



        internal string Identifier
        {
            get { return mIdentifier; }
        }



        internal double Weight
        {
            get { return mWeight; }
            set { mWeight = value; }
        }

        internal bool WeightIsMktValue
        {
            get { return mWeightIsMktValue; }
            set { mWeightIsMktValue = value; }
        }


        internal uint ConstituentCount
        {
            get { return mConstituentCount; }
            set { mConstituentCount = value; }
        }

        internal double TotalReturn
        {
            get
            {
                return Weight * RateOfReturn * 0.01;
            }
        }

        internal double AdventTotalReturn
        {
            get
            {
                return mTotalReturn;
            }
        }

        internal double AdventTotalReturnAdjusted
        {
            get
            {
                return mTotalReturnAdjusted;
            }
        }


        internal string GetIdentifier( VendorFormat vendorFormat)
        {
            string identifier = "";
            switch (vendorFormat)
            {
                case VendorFormat.CONSTITUENT:
                    identifier = mTicker; break;
                case VendorFormat.SECTOR_LEVEL1:
                    identifier = mSectorLevel1; break;
                case VendorFormat.SECTOR_LEVEL2:
                    identifier = mSectorLevel2; break;
                case VendorFormat.SECTOR_LEVEL3:
                    identifier = mSectorLevel3; break;
                case VendorFormat.SECTOR_LEVEL4:
                    identifier = mSectorLevel4; break;

            }
            return (identifier);
        }

        internal void CalculateAddlContribution(double AdventVsVendorDiff)
        {
            mAdventVsVendorDiff = AdventVsVendorDiff;

            uint count = 0;
            
            switch(mVendorFormat)
            {
                case VendorFormat.CONSTITUENT:
                    count = mConstituentCount; break;
                case VendorFormat.SECTOR_LEVEL1:
                    count = mSectorLevel1Count; break;
                case VendorFormat.SECTOR_LEVEL2:
                    count = mSectorLevel2Count; break;
                case VendorFormat.SECTOR_LEVEL3:
                    count = mSectorLevel3Count; break;
                case VendorFormat.SECTOR_LEVEL4:
                    count = mSectorLevel4Count; break;
            }

            if (count > 0)
                mAddlContribution = mAdventVsVendorDiff / count;
        }

        internal void CalculateAdventAdjustedReturn()
        {
            mRateOfReturnAdjustment = 100 * (mAddlContribution / mWeight); ;
            mRateOfReturnAdjusted = mRateOfReturn + mRateOfReturnAdjustment;
            mTotalReturnAdjusted += mWeight * mRateOfReturnAdjusted * .01; 
        }

        internal void ZeroAdventTotalReturn()
        {
            mTotalReturn = 0;
            mTotalReturnAdjusted = 0;
        }

        internal void CalculateAdventTotalReturn()
        {
            mTotalReturn += mWeight * mRateOfReturn * .01;
        }


        internal IndexRow(string sIndexDate, string sIndexname, string sCUSIP, string sTicker, 
            string sSectorLevel1, string sSectorLevel2, string sSectorLevel3, string sSectorLevel4,
            string sWeight, string sSecurityReturn, VendorFormat vendorFormat )
        {
            mIndexDate = DateTime.Parse(sIndexDate);
            mIndexname = sIndexname;
            mCUSIP = sCUSIP;
            mTicker = sTicker;
            mSectorLevel1 = sSectorLevel1;
            mSectorLevel2 = sSectorLevel2;
            mSectorLevel3 = sSectorLevel3;
            mSectorLevel4 = sSectorLevel4;

            double number;

            if (Double.TryParse(sWeight, out number))
                mWeight = Convert.ToDouble(sWeight, mCultureInfo);
            else
                mWeight = 0.0;
            if (Double.TryParse(sSecurityReturn, out number))
                mRateOfReturn = Convert.ToDouble(sSecurityReturn, mCultureInfo);
            else
                mRateOfReturn = 0.0;

            switch(vendorFormat)
            {
                case VendorFormat.CONSTITUENT:
                    mConstituentCount += 1;
                    mIdentifier = mTicker;
                    break;
                case VendorFormat.SECTOR_LEVEL1:
                    mSectorLevel1Count += 1;
                    mIdentifier = sSectorLevel1;
                    break;
                case VendorFormat.SECTOR_LEVEL2:
                    mSectorLevel2Count += 1;
                    mIdentifier = sSectorLevel2;
                    break;
                case VendorFormat.SECTOR_LEVEL3:
                    mSectorLevel3Count += 1;
                    mIdentifier = sSectorLevel3;
                    break;
                case VendorFormat.SECTOR_LEVEL4:
                    mSectorLevel4Count += 1;
                    mIdentifier = sSectorLevel4;
                    break;
            }
            mVendorFormat = vendorFormat;
        }
    }
}
