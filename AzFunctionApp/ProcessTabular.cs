using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Configuration;
using Microsoft.SqlServerAnaylsisServerTabularProcessing;


namespace ProcessTabularFunction
{
    
    public static class ProcessTabular
    {
        //[FunctionName("ProcessTabular")]
        public static void Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer, TraceWriter log)
        {    
       
            log.Info($"C# Timer trigger function started at: {DateTime.Now}");
           
            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = ConfigurationManager.AppSettings["DatabaseName"]
                };

                tabularModel.ProcessModelFull();
            }
            catch (Exception e)
            {
                log.Info($"C# Timer trigger function exception: {e.ToString()}");
            }

            log.Info($"C# Timer trigger function finished at: {DateTime.Now}");
        }
    }
}
