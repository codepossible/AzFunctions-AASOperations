using System;
using System.Configuration;
using System.Net;
using System.Net.Http;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace AzFunctionApp
{
    using Models;

    /// <summary>
    /// Azure Function to process the specified partition for the specified table in the specified database.
    /// </summary>
    public static class ProcessTabularModelProcessPartitionAysnc
    {
        [FunctionName("AsyncProcessPartition")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/partitions/{partitionName}/async")]
                HttpRequestMessage req, 
                string databaseName,
                string tableName, 
                string partitionName,
                 [Queue("%ProcessPartitionQueue%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> queue,
                 [Table("%ProcessPartitionStatusTable%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> statusTable,
                TraceWriter log)
        {
            log.Info("Received request to process specific partition in " + databaseName + "/" + tableName + "/" + partitionName);

            string outputMediaType = ConfigurationManager.AppSettings["ProcessingTrackingOutputMediaType"];
           
            QueueMessageProcesssTabular queuedMessage = null;

            try
            {
                DateTime enqueuedDateTime = DateTime.UtcNow;
                string trackingId = Guid.NewGuid().ToString();

                queuedMessage = new QueueMessageProcesssTabular()
                {
                    TrackingId = trackingId,
                    EnqueuedDateTime = enqueuedDateTime,
                    Database = databaseName,
                    Table = tableName,
                    TargetDate = DateTime.Now,
                    Parition = partitionName,
                    Status = "Queued",
                    PartitionKey = enqueuedDateTime.ToString("yyyy-MM-dd"),
                    RowKey = trackingId,
                    ETag = "*"
                };

                queue.Add(queuedMessage);
                statusTable.Add(queuedMessage);
            }
            catch (Exception e)
            {
                log.Info($"C# Timer trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            if (String.IsNullOrEmpty(outputMediaType))
            {
                return req.CreateResponse(HttpStatusCode.OK, queuedMessage.ToProcessingTrackingInfo());
            }
            else { return req.CreateResponse(HttpStatusCode.OK, queuedMessage.ToProcessingTrackingInfo(), outputMediaType); }

        }
    }
}
