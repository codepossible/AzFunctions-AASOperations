using System;
using System.Configuration;
using System.Net;
using System.Net.Http;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzFunctionApp
{    
    using Models;
 
    public static class GetProcessingStatus
    {
        [FunctionName("GetProcessingStatus")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, 
                          "get", "post", 
                           Route = "ProcessTabularModel/processing/status/{operation}/{statusTablePartitionKey}/{trackingId}")] HttpRequestMessage req,          
                            string operation,
                            string statusTablePartitionKey, 
                            string trackingId,
                            TraceWriter log)
        {
            log.Info($"Received request to find status for {operation} process with tracking information: {statusTablePartitionKey}/{trackingId}");

            string outputMediaType = ConfigurationManager.AppSettings["ProcessingTrackingOutputMediaType"];
            
            CloudTable statusTable = GetCloudTableByOperation(operation);

            if (statusTable != null) {
                try
                {

                    TableOperation retrieveOperation = TableOperation.Retrieve<QueueMessageProcesssTabular>(statusTablePartitionKey, trackingId);
                    TableResult retrievedResult = statusTable.Execute(retrieveOperation);

                    if (retrievedResult.Result != null)
                    {
                        QueueMessageProcesssTabular processStatus = (QueueMessageProcesssTabular)retrievedResult.Result;
                        if (String.IsNullOrEmpty(outputMediaType)) {
                            return req.CreateResponse(HttpStatusCode.OK, processStatus.ToProcessingTrackingInfo());
                         } else { return req.CreateResponse(HttpStatusCode.OK, processStatus.ToProcessingTrackingInfo(), outputMediaType); }
                    }
                    else
                    {
                        return req.CreateResponse(HttpStatusCode.NotFound);
                    }
                }
                catch (Exception e)
                {
                    return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
                }
            }
            else
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "Unknown Operation specified");
            }
        }

        private static CloudTable GetCloudTableByOperation(string operation)
        {
            CloudTable statusTable = null;

            string tableName = null;

            switch (operation.ToLower())
            {
                case "nextbatch":
                    tableName = ConfigurationManager.AppSettings["ProcessNextBatchStatusTable"];
                    break;
                case "table":
                    tableName = ConfigurationManager.AppSettings["ProcessTableStatusTable"];
                    break;
                case "partition":
                    tableName = ConfigurationManager.AppSettings["ProcessPartitionStatusTable"];
                    break;
                default:
                    break;
            }

            if (!String.IsNullOrEmpty(tableName))
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AzureWebJobsStorage"]);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                statusTable = tableClient.GetTableReference(tableName);
            }
            return statusTable;
        }
    }
    }
