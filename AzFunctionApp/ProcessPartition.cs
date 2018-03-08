using System;
using System.Configuration;
using System.Net;
using System.Net.Http;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

using Microsoft.SqlServerAnaylsisServerTabularProcessing;

namespace AzFunctionApp
{
    /// <summary>
    /// Azure Function to process the specified partition for the specified table in the specified database.
    /// </summary>
    public static class ProcessPartition
    {
        /// <summary>
        /// Processes the specified partition in the specified table in the specified tabular database.
        /// </summary>
        /// <param name="req">HTTP request</param>
        /// <param name="databaseName">Name of the tabular model database</param>
        /// <param name="tableName">Name of the table</param>
        /// <param name="partitionName">Name of the parition</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the result of the processing of the partition</returns>
        [FunctionName("ProcessPartition")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/partitions/{partitionName}")]
                HttpRequestMessage req, 
                string databaseName,
                string tableName, 
                string partitionName, 
                TraceWriter log)
        {
            log.Info($"Received request to process specific partition in {databaseName}/{tableName}{partitionName} asynchronously.");

            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                tabularModel.ProcessPartition(tableName, partitionName);
            }
            catch (Exception e)
            {
                log.Info($"C# Timer trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }
            
            return req.CreateResponse(HttpStatusCode.OK, "Processed partition: " + databaseName + "/" +  tableName + "/" + partitionName);
      
        }
    }
}
