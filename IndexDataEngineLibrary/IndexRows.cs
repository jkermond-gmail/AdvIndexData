using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AdventUtilityLibrary;

namespace IndexDataEngineLibrary
{
    public static class IndexRows
    {
        public static uint ConstituentCount { get; set; }
        public static uint SectorLevel1Count { get; set; }
        public static uint SectorLevel2Count { get; set; }
        public static uint SectorLevel3Count { get; set; }
        public static uint SectorLevel4Count { get; set; }

        public static double TotalReturn { get; set; }
        public static double TotalReturnAdjusted { get; set; }
        public static double AdventVsVendorDiff { get; set; }
        public static double AddlContribution { get; set; }

        public static void Reset()
        {
            ConstituentCount = 0;
            SectorLevel1Count = 0;
            SectorLevel2Count = 0;
            SectorLevel3Count = 0;
            TotalReturn = 0;
            TotalReturnAdjusted = 0;
            AdventVsVendorDiff = 0;
            AddlContribution = 0;
            return;
        }

        public static void ZeroAdventTotalReturn()
        {
            TotalReturn = 0;
            TotalReturnAdjusted = 0;
        }


        public static void CalculateAddlContribution(double adventVsVendorDiff, string sVendorFormat, bool logReturnData)
        {
            AdventVsVendorDiff = adventVsVendorDiff;

            uint count = 0;

            switch (sVendorFormat)
            {
                case "CONSTITUENT":
                    count = ConstituentCount; break;
                case "SECTOR_LEVEL1":
                    count = SectorLevel1Count; break;
                case "SECTOR_LEVEL2":
                    count = SectorLevel2Count; break;
                case "SECTOR_LEVEL3":
                    count = SectorLevel3Count; break;
                case "SECTOR_LEVEL4":
                    count = SectorLevel4Count; break;
            }

            if (count > 0)
                AddlContribution = (AdventVsVendorDiff / count);

            if (logReturnData)
            {
                LogHelper.WriteLine("AddlContribution = (AdventVsVendorDiff / count);");
                LogHelper.WriteLine("AddlContribution   " + AddlContribution.ToString());
                LogHelper.WriteLine("AdventVsVendorDiff " + AdventVsVendorDiff.ToString());
                LogHelper.WriteLine("count              " + count.ToString());
            }

        }

        public static double AdventTotalReturn
        {
            get
            {
                return TotalReturn;
            }
        }

        public static double AdventTotalReturnAdjusted
        {
            get
            {
                return TotalReturnAdjusted;
            }
        }


    }
}
