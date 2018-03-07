using System;
using System.Configuration;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.SqlServerAnaylsisServerTabularProcessing;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

namespace AzFunctionApp
{
    using Models;
    
    /// <summary>
    /// Azure function to dequeue requests to process tables and process them.
    /// </summary>
    public static class DequeueProcessTable
    {
        [FunctionName("DequeueProcessTableRequest")]
        [Singleton]
        public static void Run([QueueTrigger("%ProcessTableQueue%", Connection = "AzureWebJobsStorage")]string myQueueItem,
            [Table("%ProcessTableStatusTable%", Connection = "AzureWebJobsStorage")] CloudTable statusTable,
            TraceWriter log)
        {
            log.Info($"Received Queue trigger to process table : {myQueueItem}");

            QueueMessageProcesssTabular queueMessage = null;

            try
            {
                queueMessage = JsonConvert.DeserializeObject<QueueMessageProcesssTabular>(myQueueItem);

                int maximumRetries = int.TryParse(ConfigurationManager.AppSettings["MaximumRetries"], out maximumRetries) ?
                                        maximumRetries : 0;

                int waitTimeinSeconds = int.TryParse(ConfigurationManager.AppSettings["WaitTimeInSeconds"], out waitTimeinSeconds) ?
                                        waitTimeinSeconds : 30;

                RetryWaitPattern retryWaitPattern = Enum.TryParse<RetryWaitPattern>(ConfigurationManager.AppSettings["RetryWaitPattern"], out retryWaitPattern) ?
                                                        retryWaitPattern : RetryWaitPattern.Equal;


                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = queueMessage.Database ?? ConfigurationManager.AppSettings["DatabaseName"],
                    NumberOfRetries = maximumRetries,
                    WaitPattern = retryWaitPattern,
                    WaitTimeInSecondsBetweenRetries = waitTimeinSeconds
                };

                queueMessage.Status = "Running";
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

                tabularModel.ProcessTable(queueMessage.Table);

                queueMessage.Status = "Complete";
                queueMessage.ETag = "*";
                updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

            }
            catch (Exception e)
            {
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
                log.Error("Error occured processing tabular model", e);
                queueMessage.Status = "Error Processing";
                queueMessage.ErrorDetails = e.ToString();
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);
            }

            log.Info("Completed Table processing for " + queueMessage?.Database + "/" + queueMessage?.Table);

        }
    }
}
