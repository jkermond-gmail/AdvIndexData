/****** Script for SelectTopNRows command from SSMS  ******/
use IndexData

SELECT j.ClientID, j.JobName, j.InputFormat, j.JobType, v.Dataset , v2.AdventIndexName, i.IndexName, v2.AxmlConstituentFile, v2.AxmlSectorFile   
  FROM VendorIndexMap v
  inner join Jobs j on j.DataSet = v.Dataset 
  inner join JobIndexIds i on i.JobName = j.JobName
  inner join VendorIndexMap v2 on v2.VendorIndexName = i.IndexName
  where v.AdventIndexName = 'r3000' and j.Active = 'Yes' and j.JobType = 'Client'
  order by ClientID


  select j.*, i.*, v.AdventIndexName, v.AxmlConstituentFile, v.AxmlSectorFile
  from jobs j
  inner join JobIndexIds i on (i.JobName = j.JobName and i.ClientID = j.ClientID and i.Vendor = j.Vendor)
  inner join VendorIndexMap v on v.VendorIndexName = i.IndexName
  where j.JobName = 'S&P Security 500' and i.JobName = 'S&P Security 500'
  and j.Active = 'Yes' and j.JobType = 'Client' and j.Vendor = 'StandardAndPoors' and v.AdventIndexName = 'sp500'
  order by j.ClientID

select j.*, i.*, v.AdventIndexName, v.AxmlConstituentFile, v.AxmlSectorFile
  from jobs j
  inner join JobIndexIds i on (i.JobName = j.JobName and i.ClientID = j.ClientID and i.Vendor = j.Vendor)
  inner join VendorIndexMap v on v.VendorIndexName = i.IndexName
  where j.JobName = 'Russell Security' and i.JobName = 'Russell Security'
  and j.Active = 'Yes' and j.JobType = 'Client' and j.Vendor = 'Russell' and v.AdventIndexName = 'r3000'
  order by j.ClientID


/*select j.*, i.*, v.* */
select j.ClientID, j.JobName, j.LastProcessDate, i.IndexName, v.AdventIndexName, v.AxmlConstituentFile, v.AxmlSectorFile
 from jobs j
  inner join JobIndexIds i on (i.JobName = j.JobName and i.ClientID = j.ClientID and i.Vendor = j.Vendor)
  inner join VendorIndexMap v on v.VendorIndexName = i.IndexName
 where j.DataSet = 'sp500' and j.InputFormat = 'StandardPoorsSecurity' 
 and j.Active = 'Yes' and j.JobType = 'Client'  and j.Vendor = 'StandardAndPoors' and v.AdventIndexName = 'sp500'
 order by j.ClientID

select j.ClientID, j.JobName, j.LastProcessDate, i.IndexName, v.AdventIndexName, v.AxmlConstituentFile, v.AxmlSectorFile
 from jobs j
  inner join JobIndexIds i on (i.JobName = j.JobName and i.ClientID = j.ClientID and i.Vendor = j.Vendor)
  inner join VendorIndexMap v on v.VendorIndexName = i.IndexName
 where j.DataSet = 'RGS' and j.InputFormat = 'RussellSecurity' 
 and j.Active = 'Yes' and j.JobType = 'Client'  and j.Vendor = 'Russell' and v.AdventIndexName = 'r3000'
 order by j.ClientID
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
  /*

       public void CopyFilesToFtpFolder(string sFileDate, Vendors vendor, string dataSet, string sIndexName, AdventOutputType outputType)
         {

        }


        public void CopyFileToFtpFolder(string clientId, string sFileDate, Vendors vendor, string sIndexName, AdventOutputType outputType)
        {

   */