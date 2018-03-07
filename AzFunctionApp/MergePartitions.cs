using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.SqlServerAnaylsisServerTabularProcessing;
using System.Configuration;

using Microsoft.SqlServerAnaylsisServerTabularProcessing.Models;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace AzFunctionApp
{
    /// <summary>
    /// Azure function to merge the specified partitions into a specified target parition for the specified table in the specified database.
    /// Target partition is created, if it does not exist.
    /// </summary>
    public static class MergePartitions
    {
        [FunctionName("MergePartitions")]
        public static  async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function,  
                "put", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/merge")]HttpRequestMessage req, 
            string databaseName,
            string tableName,
            TraceWriter log)
        {
            log.Info("Received request to merge partitions in " + databaseName + "/" + tableName);
            try
            {
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                //read the request content and de-serialize the JSON payload of new partition information
                string mergePartitionInfoSerialized = await req.Content.ReadAsStringAsync();
                MergeParitionInfo mergePartitionInfo = JsonConvert.DeserializeObject<MergeParitionInfo>(mergePartitionInfoSerialized);

                tabularModel.MergeParitions(tableName, mergePartitionInfo.TargetPartition, mergePartitionInfo.SourcePartitionNames);
            }
            catch (Exception e)
            {
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            return req.CreateResponse(HttpStatusCode.OK, "Merged specified paritions in: " + databaseName + "/" + tableName);
        }
    }
}
