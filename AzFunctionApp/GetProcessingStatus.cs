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
 
    /// <summary>
    /// Azure function to return the status of the processing
    /// </summary>
    public static class GetProcessingStatus
    {

        /// <summary>
        /// Returns the status of the asynchronous processing
        /// </summary>
        /// <param name="req">HTTP Request</param>
        /// <param name="operation">Operation that is being tracked - model | table | partition </param>
        /// <param name="statusTablePartitionKey">Partition key from the tracking information</param>
        /// <param name="trackingId">tracking id from the tracking information</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Updated information about the processing or error, if not found or bad request </returns>
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
                        /* Return result if found */
                        QueueMessageProcesssTabular processStatus = (QueueMessageProcesssTabular)retrievedResult.Result;
                        log.Info($"Found status for {operation} process with tracking information: {statusTablePartitionKey}/{trackingId} | Status: {processStatus.Status}");
                        return req.CreateResponse(HttpStatusCode.OK, processStatus.ToProcessingTrackingInfo());                         
                    }
                    else
                    {
                        /*  Return not found error if tracking info was not found */
                        log.Info($"Could not find status for {operation} process with tracking information: {statusTablePartitionKey}/{trackingId}");
                        return req.CreateResponse(HttpStatusCode.NotFound);
                    }
                }
                catch (Exception e)
                {
                    log.Error($"Error occurred retrieving status for {operation} process with tracking information: {statusTablePartitionKey}/{trackingId}", e);
                    return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
                }
            }
            else
            {
                /* invalid status table */
                var errorMessage = $"Unknown operation - {operation}";
                log.Info(errorMessage);
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, errorMessage);
            }
        }

        /// <summary>
        /// Returns the reference to Azure Table Storage for the operation type
        /// </summary>
        /// <param name="operation">Name of the operation in the request</param>
        /// <returns>Reference of the Azure Table Storage</returns>
        private static CloudTable GetCloudTableByOperation(string operation)
        {
            CloudTable statusTable = null;

            string tableName = null;

            switch (operation.ToLower())
            {
                case "model":
                    tableName = ConfigurationManager.AppSettings["ProcessModelStatusTable"];
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
