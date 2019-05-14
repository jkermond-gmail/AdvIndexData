using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Mail;

namespace AdventUtilityLibrary
{
    public sealed class Mail
    {
        //public static void SendMail(MailMessage Message)
        //{
        //    SmtpClient client = new SmtpClient();
        //    client.Host = "smtp.googlemail.com";
        //    client.Port = 587;
        //    client.UseDefaultCredentials = false;
        //    client.DeliveryMethod = SmtpDeliveryMethod.Network;
        //    client.EnableSsl = true;
        //    client.Credentials = new NetworkCredential("myemail@gmail.com", "password");
        //    client.Send(Message);
        //}

        public void SendMail2(string sMessage)
        { 
            SmtpClient client = new SmtpClient();
            client.Host = "10.128.8.25";
            client.Timeout = 10000;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;

            MailMessage mm = new MailMessage("hmd@hubdata.com", "jkermond@gmail.com", "test", "test");
                    mm.BodyEncoding = UTF8Encoding.UTF8;
            mm.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;

            client.Send(mm);
        }

        public void SendMail(string sMessage)
        {
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient();
            mail.To.Add("jkermond@gmail.com");
            mail.From = new MailAddress("advindexdata@hubdata.com");
            mail.Subject = "test subject";
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
