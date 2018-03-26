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
    /// Azure function to queue the request to process the table
    /// </summary>
    public static class ProcessTableAsync
    {
        /// <summary>
        /// Queues the request to process the specified table in the specified database.
        /// </summary>
        /// <param name="req">HTTP request</param>
        /// <param name="databaseName">Name of the tabular database</param>
        /// <param name="tableName">Name of the table to process</param>
        /// <param name="queue">Queue to place the procesing request</param>
        /// <param name="statusTable">Table to track the status of the processing request</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the tracking information for the procesing request</returns>
        [FunctionName("AsyncProcessTable")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, "get",
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/async")]HttpRequestMessage req,
                    string databaseName,
                    string tableName,
                    [Queue("%ProcessTableQueue%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> queue,
                    [Table("%ProcessTableStatusTable%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> statusTable,
                    TraceWriter log)
        {
            log.Info($"Received request to queue processing the table - {databaseName}/{tableName}");

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
                    Parition = null,
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
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
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
