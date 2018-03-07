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
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }
            
            return req.CreateResponse(HttpStatusCode.OK, "Processed table: " + databaseName + "/" + tableName);            
        }
    }
}
