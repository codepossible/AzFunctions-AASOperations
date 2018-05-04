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
        /// <summary>
        /// Process the specficed table in the specified tabular model based on requests from Azure Queue
        /// </summary>
        /// <param name="myQueueItem">QueueItem</param>
        /// <param name="statusTable">Azure Table to store the status information</param>
        /// <param name="log">Instance of log writer</param>
        [FunctionName("DequeueProcessTableRequest")]
        [Singleton]
        public static void Run([QueueTrigger("%ProcessTableQueue%", Connection = "AzureWebJobsStorage")]string myQueueItem,
            [Table("%ProcessTableStatusTable%", Connection = "AzureWebJobsStorage")] CloudTable statusTable,
            TraceWriter log)
        {
            log.Info($"Received queue trigger to process table : {myQueueItem}");

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

                log.Info($"Starting table processing on {queueMessage.Database}/{queueMessage.Tables}");

                if (queueMessage.Tables.Contains(","))
                {

                    log.Info($"Multiple table processing requested.");

                    var tableNames = queueMessage.Tables.Split(',');
                    if (tableNames?.Length > 0)
                    {
                        log.Info($"Sending request to process {tableNames?.Length} tables in {queueMessage.Database}.");
                        tabularModel.ProcessTables(tableNames);
                    }
                }
                else
                {
                    log.Info($"Single table processing requested.");
                    tabularModel.ProcessTable(queueMessage.Tables);
                }

                queueMessage.Status = "Complete";
                queueMessage.ETag = "*";
                updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

            }
            catch (Exception e)
            {                
                log.Error($"Error occured processing database table - {queueMessage?.Database}/{queueMessage?.Tables} : {e.ToString()}", e);
                queueMessage.Status = "Error Processing";
                queueMessage.ErrorDetails = e.ToString();
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);
            }

            log.Info($"Completed table processing for {queueMessage?.Database}/{queueMessage?.Tables}");

        }
    }
}
