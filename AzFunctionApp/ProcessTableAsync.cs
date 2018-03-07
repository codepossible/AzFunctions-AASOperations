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

    public static class ProcessTableAsync
    {
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
            log.Info("Received request to queue processing the table - " + databaseName + "/" + tableName);

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
