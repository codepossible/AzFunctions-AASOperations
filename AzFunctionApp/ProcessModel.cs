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
    /// Azure function to process the specified database.
    /// </summary>
    public static class ProcessModel
    {
        /// <summary>
        /// Process the specified tabular model.
        /// </summary>
        /// <param name="req">HTTP request</param>
        /// <param name="databaseName">Name of the database to process</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Returns the result of the processing of the model</returns>
        [FunctionName("ProcessModel")]
        public static HttpResponseMessage Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", 
            Route = "ProcessTabularModel/{databaseName}")]HttpRequestMessage req,
                string databaseName,
                TraceWriter log)
        {
            log.Info("Received request to process the model " + databaseName);           
            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                tabularModel.ProcessModelFull();                
            }
            catch (Exception e)
            {
                log.Error($"Error processing database - {databaseName}: {e.ToString()}", e);
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            var successMessage = $"Successfully processed database - {databaseName}";
            log.Info(successMessage);
            return req.CreateResponse(HttpStatusCode.OK, new { result = successMessage });
        }
    }
}
