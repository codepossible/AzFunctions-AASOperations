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
        /// <param name="tableList">Single table name or List of comma seperated table names</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the result of the processing of the table</returns>
        [FunctionName("ProcessTable")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableList}")]HttpRequestMessage req, 
                    string databaseName, 
                    string tableList,
                    TraceWriter log)
        {
            log.Info($"Received request to process the table - {databaseName}/{tableList}");

            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                log.Info($"Starting table processing on {databaseName}/{tableList}");

                if (tableList.Contains(","))
                {

                    log.Info($"Multiple table processing requested.");

                    var tableNames = tableList.Split(',');
                    if (tableNames?.Length > 0)
                    {
                        log.Info($"Sending request to process {tableNames?.Length} tables in {databaseName}.");
                        tabularModel.ProcessTables(tableNames);
                    }
                }
                else
                {
                    log.Info($"Single table processing requested.");
                    tabularModel.ProcessTable(tableList);
                }
            }
            catch (Exception e)
            {
                log.Error($"Error occured processing {databaseName}/{tableList}. Details: {e.ToString()}", e);
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            var successMessage = $"Successfully processed table - {databaseName}/{tableList}";
            log.Info(successMessage);
            return req.CreateResponse(HttpStatusCode.OK, new { result = successMessage });            
        }
    }
}
