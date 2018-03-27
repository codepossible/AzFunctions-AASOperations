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
    /// Azure function to dequeue requests to process tabular model and process them.
    /// </summary>
    public static class DequeueProcessModel
    {
        /// <summary>
        /// Process the specified tabular model based on requests from Azure Queue
        /// </summary>
        /// <param name="myQueueItem">QueueItem</param>
        /// <param name="statusTable">Azure Table to store the status information</param>
        /// <param name="log">Instance of log writer</param>
        [FunctionName("DequeueProcessModelRequest")]
        public static void Run([QueueTrigger("%ProcessModelQueue%", Connection = "AzureWebJobsStorage")]string myQueueItem,
            [Table("%ProcessModelStatusTable%", Connection = "AzureWebJobsStorage")] CloudTable statusTable,
            TraceWriter log)
        {
            log.Info($"Received queue trigger to process database : {myQueueItem}");

            QueueMessageProcesssTabular queueMessage = null;

            try
            {
                queueMessage = JsonConvert.DeserializeObject<QueueMessageProcesssTabular>(myQueueItem);

                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = queueMessage.Database ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                queueMessage.Status = "Running";
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

                tabularModel.ProcessModelFull();

                queueMessage.Status = "Complete";
                queueMessage.ETag = "*";
                updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

            }
            catch (Exception e)
            {
                log.Error($"Error occured processing database - {queueMessage?.Database} : {e.ToString()}", e);
                queueMessage.Status = "Error Processing";
                queueMessage.ErrorDetails = e.ToString();
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);
            }

            log.Info($"Successfully completed database processing for  + {queueMessage?.Database}");
        }
    }
}
