using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.SqlServerAnaylsisServerTabularProcessing;
using System.Configuration;
using System;

namespace AzFunctionApp
{
    /// <summary>
    /// Azure function to process the specified table in the specified database.
    /// </summary>
    public static class ProcessTable
    {
        /// <summary>
        /// Processes the specified table in the specified tabular database.
        /// </summary>
        /// <param name="req">HTTP request</param>
        /// <param name="databaseName">Name of the tabular model database</param>
        /// <param name="tableName">Name of the table</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the result of the processing of the table</returns>
        [FunctionName("ProcessTable")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}")]HttpRequestMessage req, 
                    string databaseName, 
                    string tableName,
                    TraceWriter log)
        {
            log.Info("Received request to process the table - " + databaseName + "/" + tableName);

            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                tabularModel.ProcessTable(tableName);
            }
            catch (Exception e)
            {
                log.Info($"Error occured processing {databaseName}/{tableName}. Details: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            log.Info($"Successfully processed - {databaseName}/{tableName}");
            return req.CreateResponse(HttpStatusCode.OK, new { result = $"Successfully Processed {databaseName}/{tableName}" });            
        }
    }
}
