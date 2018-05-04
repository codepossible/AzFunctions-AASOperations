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
    /// Azure function to dequeue requests to process partitions and process them.
    /// </summary>
    public static class DequeueProcessPartition
    {
        /// <summary>
        /// Process the specified partition in the specficed table in the specified tabular model based on requests from Azure Queue
        /// </summary>
        /// <param name="myQueueItem">QueueItem</param>
        /// <param name="statusTable">Azure Table to store the status information</param>
        /// <param name="log">Instance of log writer</param>
        [FunctionName("DequeueProcessPartitionRequest")]
        public static void Run([QueueTrigger("%ProcessPartitionQueue%", Connection = "AzureWebJobsStorage")]string myQueueItem,
            [Table("%ProcessPartitionStatusTable%", Connection = "AzureWebJobsStorage")] CloudTable statusTable,
            TraceWriter log)
        {
            log.Info($"Received queue trigger to process partition : {myQueueItem}");

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

                tabularModel.ProcessPartition(queueMessage.Tables, queueMessage.Parition);

                queueMessage.Status = "Complete";
                queueMessage.ETag = "*";
                updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);

            }
            catch (Exception e)
            {                
                log.Error($"Error occured processing partition - " +
                    $"{queueMessage?.Database}/{queueMessage?.Tables}/{queueMessage?.Parition} : {e.ToString()}", e);
                queueMessage.Status = "Error Processing";
                queueMessage.ErrorDetails = e.ToString();
                queueMessage.ETag = "*";
                TableOperation updateOperation = TableOperation.InsertOrReplace(queueMessage);
                statusTable.Execute(updateOperation);
            }

            log.Info($"Successfully completed partition processing for  {queueMessage?.Database}/{queueMessage?.Tables}/{queueMessage?.Parition}");

        }
    }
}
