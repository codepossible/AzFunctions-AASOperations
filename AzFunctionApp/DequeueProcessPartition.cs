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
        [FunctionName("DequeueProcessPartitionRequest")]
        public static void Run([QueueTrigger("%ProcessPartitionQueue%", Connection = "AzureWebJobsStorage")]string myQueueItem,
            [Table("%ProcessPartitionStatusTable%", Connection = "AzureWebJobsStorage")] CloudTable statusTable,
            TraceWriter log)
        {
            log.Info($"Received Queue trigger to process partition : {myQueueItem}");

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

                tabularModel.ProcessPartition(queueMessage.Table, queueMessage.Parition);

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
