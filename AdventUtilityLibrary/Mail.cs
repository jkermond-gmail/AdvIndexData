﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Mail;

namespace AdventUtilityLibrary
{
    public sealed class Mail
    {
        bool mSendMail = false; 

        public Mail()
        {
            mSendMail = AppSettings.Get<bool>("sendMail");
        }

        public void SendMail(string sMessage)
        {
            string subject = "AdvIndexData Status";
            LogHelper.WriteLine("Send mail " + subject);
            LogHelper.WriteLine("Send mail " + sMessage);

            if (mSendMail.Equals(true))
            {
                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient();
                mail.To.Add("jkermond@sscinc.com");
                mail.To.Add("ebytchkova@sscinc.com");
                mail.From = new MailAddress("advindexdata@hubdata.com");
                mail.Subject = subject;
                mail.IsBodyHtml = false;
                mail.Body = sMessage;
                SmtpServer.Host = "10.128.8.25";
                SmtpServer.Port = 25;
                SmtpServer.DeliveryMethod = System.Net.Mail.SmtpDeliveryMethod.Network;
                try
                {
                    SmtpServer.Send(mail);
                }
                catch (Exception ex)
                {
                    LogHelper.WriteLine("Mail Exception Message: " + ex.Message);
                    if (ex.InnerException != null)
                        LogHelper.WriteLine("Mail Exception Inner:   " + ex.InnerException);
                }
            }
        }
    }
}
