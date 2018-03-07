using System;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

using Newtonsoft.Json;

using Microsoft.SqlServerAnaylsisServerTabularProcessing;
using Microsoft.SqlServerAnaylsisServerTabularProcessing.Models;


namespace AzFunctionApp
{
    /// <summary>
    /// Azure function to create new partitions based on specified partition information in the specified table in the specified database.
    /// </summary>
    public static class CreateNewPartitions
    {
        [FunctionName("CreateNewPartitions")]
        public async static Task<HttpResponseMessage> Run([
            HttpTrigger(AuthorizationLevel.Function, "post", 
                Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/partitions/new")]HttpRequestMessage req, 
            string databaseName,  
            string tableName,           
            TraceWriter log)
        {
            log.Info("Received request to create new partitions in " + databaseName + "/" + tableName);

            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                //read the request content and de-serialize the JSON payload of new partition information
                string partitionInfoSerialized = await req.Content.ReadAsStringAsync();
                NewPartitionInfo[] partitionInfoList = JsonConvert.DeserializeObject<NewPartitionInfo[]>(partitionInfoSerialized);

                if (partitionInfoList != null && partitionInfoList.Length > 0)
                {
                    tabularModel.CreatePartitions(tableName, partitionInfoList);
                }
            }
            catch (Exception e)
            {
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            return req.CreateResponse(HttpStatusCode.OK, "Created partitions on " + databaseName);
        }
    }
}
