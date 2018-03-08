using System;
using System.Collections.Generic;

namespace ProcessTabularFunction.Utility
{

    /// <summary>
    /// Partition Granularity 
    /// </summary>
    public enum PartitionGranularity
    {
        Undefined = -1,
        Yearly = 0,
        Monthly = 1,
        Daily = 2
    }

    /// <summary>
    /// Helper class to encapuslate in common operations across Azure functions
    /// </summary>
    public static class QueryHelper
    {
        /// <summary>
        /// Set of Source queries for various tables
        /// </summary>
        public static readonly Dictionary<String, String> TableProcessQuerySet = new Dictionary<String, String> {     
            /* Example for AdventureWorks DB */
            { "metrics-internet-sales", "SELECT * FROM [dbo].[FactInternetSales] WHERE [OrderDateKey] >= {0} and [OrderDateKey] < {1}" }           
        };


        /// <summary>
        /// Generates the partition tool by the given date.
        /// </summary>
        /// <param name="partitionBeginDate">Begin date for partition</param>
        /// <param name="partitionGranularity">Partition Granularity - Yearly, Monthly (default), Daily or undefined </param>
        /// <returns>Generates the partition date and granularity</returns>
        public static string GeneratePartitionKey(
                                DateTime? partitionBeginDate = null, 
                                PartitionGranularity partitionGranularity = PartitionGranularity.Monthly)
        {
            if (!partitionBeginDate.HasValue || partitionBeginDate == DateTime.MinValue) { partitionBeginDate = DateTime.Today; }

            int partitionKey = 0;

            switch (partitionGranularity)
            {
                case PartitionGranularity.Yearly:
                    partitionKey = partitionBeginDate.Value.Year;
                    break;

                case PartitionGranularity.Monthly:
                    partitionKey = partitionBeginDate.Value.Year * 100 + partitionBeginDate.Value.Month;
                    break;

                case PartitionGranularity.Daily:
                    partitionKey = partitionBeginDate.Value.Year * 10000
                                    + partitionBeginDate.Value.Month * 100
                                    + partitionBeginDate.Value.Day;
                    break;

                case PartitionGranularity.Undefined:
                    partitionKey = -1;
                    break;

                default:
                    partitionKey = 0;
                    break;
            }            

            return partitionKey.ToString();
        }

        

        /// <summary>
        /// Returns the source query for the specified table and partition begin date
        /// </summary>
        /// <param name="tableName">Table to partition</param>
        /// <param name="partitionBeginDate">Begin date for partition</param>
        /// <returns></returns>
        public static string GetSourceQueryBasedOnDate(
            string tableName,
            DateTime? partitionBeginDate = null, 
            PartitionGranularity partitionGranularity = PartitionGranularity.Monthly)
        {
            string sourceQuery = null; 

            if (!partitionBeginDate.HasValue || partitionBeginDate == DateTime.MinValue) { partitionBeginDate = DateTime.Today; }

            string queryFormat = TableProcessQuerySet[tableName.ToLower()];

          
            // Determine if the query requires calculation of a date range 
            bool dateRangeParametersRequired = queryFormat != null 
                                                && queryFormat.Contains("{0}") 
                                                && queryFormat.Contains("{1}");

            if (dateRangeParametersRequired)
            {               
                sourceQuery = BuildDateRangeParameterizedQuery(partitionBeginDate, partitionGranularity, queryFormat);
            }
            else
            {
                sourceQuery = queryFormat;
            }

            return sourceQuery;
        }


        /// <summary>
        /// Helper function to build the source query to include date ranges based on specified granularity
        /// </summary>
        /// <param name="partitionBeginDate"></param>
        /// <param name="partitionGranularity"></param>
        /// <param name="queryFormat"></param>
        /// <returns></returns>
        private static string BuildDateRangeParameterizedQuery(
                                    DateTime? partitionBeginDate, 
                                    PartitionGranularity partitionGranularity, 
                                    string queryFormat)
        {
            string sourceQuery;

            int beginDateKey = 0;
            int endDateKey = int.MaxValue;

            switch (partitionGranularity)
            {
                case PartitionGranularity.Yearly:
                    // January 1 of the year of the specified date
                    beginDateKey = partitionBeginDate.Value.Year * 10000
                    + 1 * 100
                    + 1;

                    // December 31 of the year of the specified date
                    endDateKey = partitionBeginDate.Value.Year * 10000
                                        + 12 * 100
                                        + 31;

                    break;

                case PartitionGranularity.Monthly:
                    // first day of the month of the specified date
                    beginDateKey = partitionBeginDate.Value.Year * 10000
                                            + partitionBeginDate.Value.Month * 100
                                            + 1;

                    // last day of the month of the specified date
                    endDateKey = partitionBeginDate.Value.Year * 10000
                                        + partitionBeginDate.Value.Month * 100
                                        + DateTime.DaysInMonth(partitionBeginDate.Value.Year, partitionBeginDate.Value.Month);
                    break;

                case PartitionGranularity.Daily:
                    // Specified date
                    beginDateKey = partitionBeginDate.Value.Year * 10000
                                            + partitionBeginDate.Value.Month * 100
                                            + partitionBeginDate.Value.Day;

                    // Next day from the of the specified date
                    DateTime nextDay = partitionBeginDate.Value.AddDays(1);
                    endDateKey = nextDay.Year * 10000
                                        + nextDay.Month * 100
                                        + nextDay.Day;
                    break;
            }

            sourceQuery = String.Format(queryFormat, beginDateKey, endDateKey);
            return sourceQuery;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="targetDate"></param>
        /// <returns></returns>
        public static string[] GetDailyPartitionNamesForMonthByDate(DateTime? targetDate = null)
        {
            List<string> dailyPartitionNames = new List<string>();

            if (!targetDate.HasValue || targetDate == DateTime.MinValue) { targetDate = DateTime.Today; }

             for (int dayCount = 1; 
                   dayCount <= DateTime.DaysInMonth(targetDate.Value.Year, targetDate.Value.Month);
                   dayCount++)
            {
                int dateKey = targetDate.Value.Year * 10000
                                + targetDate.Value.Month * 100
                                + dayCount;

                dailyPartitionNames.Add(dateKey.ToString());
            }


            return dailyPartitionNames.ToArray();

        }

    }
}
