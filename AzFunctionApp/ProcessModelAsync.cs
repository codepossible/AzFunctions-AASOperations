using System;
using System.Net;
using System.Net.Http;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace AzFunctionApp
{
    using Models;

    /// <summary>
    /// Azure function to process the specified tabular model database asynchronously.
    /// </summary>
    public static class ProcessModelAsync
    {
        /// <summary>
        /// Queues the request to process the specified tabular modular database.
        /// </summary>
        /// <param name="req">HTTP request</param>
        /// <param name="queue">Queue to place the procesing request</param>
        /// <param name="statusTable">Table to track the status of the processing request</param>
        /// <param name="databaseName">Name of the database</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the tracking information for the procesing request</returns>
        [FunctionName("ProcessModelAsync")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, "get",
            Route = "ProcessTabularModel/{databaseName}/async")]HttpRequestMessage req,
                [Queue("%ProcessModelQueue%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> queue,
                 [Table("%ProcessModelStatusTable%", Connection = "AzureWebJobsStorage")] ICollector<QueueMessageProcesssTabular> statusTable,
                string databaseName,
                TraceWriter log)
        {
            log.Info($"Received request to process the model {databaseName} asynchronously.");
     
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
                    Table = null,
                    TargetDate = DateTime.Now,
                    Parition = null,
                    Status = "Queued",
                    PartitionKey = enqueuedDateTime.ToString("yyyy-MM-dd"),
                    RowKey = trackingId,
                    ETag = "*"
                };

                queue.Add(queuedMessage);
                statusTable.Add(queuedMessage);

                log.Info($"Successfully queued request to process database - {databaseName} as {queuedMessage.PartitionKey}/{queuedMessage.RowKey}");
            }
            catch (Exception e)
            {
                log.Error($"Error occured trying to queue request to process database - {databaseName}. Details : {e.ToString()}", e);
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }
            
            return req.CreateResponse(HttpStatusCode.OK, queuedMessage.ToProcessingTrackingInfo());
        }
    }
}
