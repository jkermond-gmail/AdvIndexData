using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.ComponentModel;

namespace AdventUtilityLibrary
{
    public static class AppSettings
    {
        // https://stacktoheap.com/blog/2013/01/20/using-typeconverters-to-get-appsettings-in-net/
        public static T Get<T>(string key)
        {
            var appSetting = ConfigurationManager.AppSettings[key];
            //if (string.IsNullOrWhiteSpace(appSetting)) throw new AppSettingNotFoundException(key);
            if (string.IsNullOrWhiteSpace(appSetting))
            {
                if (typeof(T).Name.Equals("Int32"))
                    appSetting = "0";
                else if (typeof(T).Name.Equals("Boolean"))
                    appSetting = "false";
                else if (typeof(T).Name.Equals("String"))
                    appSetting = "";
            }
            var converter = TypeDescriptor.GetConverter(typeof(T));
            return (T)(converter.ConvertFromInvariantString(appSetting));
        }
    }
}
