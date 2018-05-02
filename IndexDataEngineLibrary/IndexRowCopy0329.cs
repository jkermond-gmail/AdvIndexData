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
            SECTOR,
            SUBSECTOR,
            INDUSTRY
        }
        private static uint mConstituentCount;
        private static uint mSectorCount;
        private static uint mSubSectorCount;
        private static uint mIndustryCount;
        private static VendorFormat mVendorFormat;

        /*
        mIndexDate = sIndexDate;
            mIndexname = sIndexname;
            mCUSIP = sCUSIP;
            mTicker = sTicker;
            mSector = sSector;
            mWeight = sWeight;
            mSecurityReturn = sSecurityReturn;
            */


        private DateTime mIndexDate;
        private string mIndexname;
        private string mCUSIP;
        private string mTicker;
        private string mSector;
        private string mSubSector;
        private string mIndustry;
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

        internal string Sector
        {
            get { return mSector; }
        }

        internal string SubSector
        {
            get { return mSubSector; }
        }

        internal string Industry
        {
            get { return mIndustry; }
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
                case VendorFormat.SECTOR:
                    identifier = mSector; break;
                case VendorFormat.SUBSECTOR:
                    identifier = mSubSector; break;
                case VendorFormat.INDUSTRY:
                    identifier = mIndustry; break;
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
                case VendorFormat.SECTOR:
                    count = mSectorCount; break;
                case VendorFormat.SUBSECTOR:
                    count = mSectorCount; break;
                case VendorFormat.INDUSTRY:
                    count = mIndustryCount; break;
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

        internal void CalculateAdventTotalReturn()
        {
            mTotalReturn += mWeight * mRateOfReturn * .01;
        }


        internal IndexRow(string sIndexDate, string sIndexname, string sCUSIP, string sTicker, 
            string sSector, string sSubSector, string sIndustry,
            string sWeight, string sSecurityReturn, VendorFormat vendorFormat )
        {
            mIndexDate = DateTime.Parse(sIndexDate);
            mIndexname = sIndexname;
            mCUSIP = sCUSIP;
            mTicker = sTicker;
            mSector = sSector;
            mSubSector = sSubSector;
            mIndustry = sIndustry;

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
                case VendorFormat.SECTOR:
                    mSectorCount += 1;
                    mIdentifier = sSector;
                    break;
                case VendorFormat.SUBSECTOR:
                    mSubSectorCount += 1;
                    mIdentifier = sSubSector;
                    break;
                case VendorFormat.INDUSTRY:
                    mIndustryCount += 1;
                    mIdentifier = sIndustry;
                    break;
            }
            mVendorFormat = vendorFormat;
        }
    }
}
