using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Net.Http;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

using Microsoft.SqlServerAnaylsisServerTabularProcessing;
using Microsoft.SqlServerAnaylsisServerTabularProcessing.Models;

namespace ProcessTabularFunction
{
    using Utility;

    /// <summary>
    /// Azure function to create the specified number of monthly partitions (default: 180 = 15 years * 12 months) in a specified table in the specified database
    /// going back from specified target date (defaults to current date, if not specified).
    /// </summary>
    public static class TableAutoRepartition
    {
        /// <summary>
        /// Repartitions the specified table into specified number of monthly paritions going back from the target date. Default Paritions count:180.
        /// </summary>
        /// <param name="req">HTTP Request</param>
        /// <param name="databaseName">Name of the database</param>
        /// <param name="tableName">Table to reparition.</param>
        /// <param name="count">Number of paritions</param>
        /// <param name="date">traget date to start building the partitions backward.</param>
        /// <param name="log">Instance of log writer</param>
        /// <returns>Success, the table is reparitioned, else error message.</returns>
        [FunctionName("TableRepartitionByDate")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "post", 
            Route = "ProcessTabularModel/{databaseName}/tables/{tableName}/repartition/{count}/bydate/{date?}")]HttpRequestMessage req, 
            string databaseName, 
            string tableName,
            string count,
            string date,
            TraceWriter log)
        {
            log.Info($"Received request to auto repartition {databaseName}/{tableName} by months.");

            DateTime targetDate = DateTime.TryParse(date, out targetDate) ? targetDate : DateTime.Today;
            int maxPartitionCount = Int32.TryParse(count, out maxPartitionCount) ? maxPartitionCount : 180;

            log.Info($"Start Date:  {targetDate} | Maximum Partitions: {maxPartitionCount}");
            
            try
            {
                // Determine number of partitions to be created by month based on number of partitions and end date and their properties
                List<NewPartitionInfo> newPartitionInfoList = new List<NewPartitionInfo>();

                for (int partitionCount = 0; 
                        partitionCount < maxPartitionCount; 
                        partitionCount++)
                {

                    DateTime currentTargetDate = targetDate.AddMonths(-partitionCount);
                    NewPartitionInfo newPartitionInfo = new NewPartitionInfo()
                    {
                        TableName = tableName,
                        PartitionName = QueryHelper.GeneratePartitionKey(currentTargetDate, PartitionGranularity.Monthly),
                        SourceQuery = QueryHelper.GetSourceQueryBasedOnDate(tableName, currentTargetDate, PartitionGranularity.Monthly)
                    };
                    newPartitionInfoList.Add(newPartitionInfo);
                }

                // Create the missing partitions in the database
                SqlServerAnalysisServerTabular tabularModel = new SqlServerAnalysisServerTabular()
                {
                    ConnectionString = ConfigurationManager.ConnectionStrings["SsasTabularConnection"].ConnectionString,
                    DatabaseName = databaseName ?? ConfigurationManager.AppSettings["DatabaseName"]
                };

                if (newPartitionInfoList.Count > 0)
                {
                    tabularModel.CreatePartitions(tableName, newPartitionInfoList.ToArray());
                }                
            }
            catch (Exception e)
            {
                log.Info($"C# HTTP trigger function exception: {e.ToString()}");
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, e);
            }

            return req.CreateResponse(HttpStatusCode.OK, 
                $"Repartitioned  {databaseName}/{tableName} + {maxPartitionCount} partitions from {targetDate.ToShortDateString()}");
        }
    }
}
