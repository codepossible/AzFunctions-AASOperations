using Microsoft.AnalysisServices.Tabular;
using System;
using System.Collections.Generic;



namespace Microsoft.SqlServerAnaylsisServerTabularProcessing
{

    using Models;
    
    public enum RetryWaitPattern
    {
        Equal = 0,
        Progressive = 1,
        RandomlyProgressive = 2
    }
    
    /// <summary>
    /// The class to process SQL Service Analysis Server Tabular
    /// </summary>
    public class SqlServerAnalysisServerTabular
    {
        /// <summary>
        /// Connection string to the SQL Server Analysis Services
        /// </summary>
        public String ConnectionString { set; get; }

        /// <summary>
        /// Database name
        /// </summary>
        public String DatabaseName { set; get; }

        /// <summary>
        /// Number of retries if error occurs
        /// </summary>
        public int NumberOfRetries { set; get; }

        /// <summary>
        /// Wait time in seconds between retries
        /// </summary>
        public int WaitTimeInSecondsBetweenRetries;

        /// <summary>
        /// Retry wait patttern
        /// </summary>
        public RetryWaitPattern WaitPattern { get; set; }


        /// <summary>
        /// Process the full model for the specified database
        /// </summary>
        public void ProcessModelFull()
        {
            Server analysisServer = new Server();

            try
            {

                analysisServer.Connect(this.ConnectionString);
                Database database = analysisServer.Databases.GetByName(this.DatabaseName);

                Model model = database.Model;
                model.RequestRefresh(RefreshType.Full);

               ModelSaveWithRetries(model);

            }
            finally { analysisServer.Disconnect(); }

        }



        /// <summary>
        /// Process the specified table in the database
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="dataOnly">flag to indicate, if only data needs to be refereshed </param>
        public void ProcessTable(string tableName, bool dataOnly = false)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(tableName);

                    if (tableToProcess != null)
                    {
                        tableToProcess.RequestRefresh(dataOnly ? RefreshType.DataOnly : RefreshType.Full);
                       ModelSaveWithRetries(model);
                    }
                }
                finally { analysisServer.Disconnect(); }
            }
        }


        /// <summary>
        /// Process the specified table in the database
        /// </summary>
        /// <param name="tableNames">Names of tables to process</param>
        /// <param name="dataOnly">flag to indicate, if only data needs to be refereshed </param>
        public void ProcessTables(string[] tableNames, bool dataOnly = false)
        {
            if (tableNames?.Length > 0)
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    int countOfTablesToProcesss = 0;

                    foreach (var tableName in tableNames)
                    {
                        Table tableToProcess = model.Tables.Find(tableName.Trim());

                        if (tableToProcess != null)
                        {
                            tableToProcess.RequestRefresh(dataOnly ? RefreshType.DataOnly : RefreshType.Full);
                            countOfTablesToProcesss++;
                        }
                    }

                    // Save models only if there are tables to process
                    if (countOfTablesToProcesss > 0) { ModelSaveWithRetries(model); }

                }
                finally { analysisServer.Disconnect(); }
            }
        }

        /// <summary>
        /// Sequentially process the partitions in the specified table
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="dataOnly">flag to indicate, if only data needs to be refereshed </param>
        public void ProcessTableByPartitions(string tableName, bool dataOnly = false)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(tableName);

                    if (tableToProcess != null)
                    {
                        if (tableToProcess.Partitions.Count > 0)
                        {
                            foreach (Partition partition in tableToProcess.Partitions)
                            {
                                ProcessPartition(tableName, partition.Name, dataOnly);
                            }
                        }
                        else
                        {
                            ProcessTable(tableName, dataOnly);
                        }
                    }
                }
                finally { analysisServer.Disconnect(); }
            }
        }

        /// <summary>
        /// Process the specified partition for the specified table
        /// </summary>
        /// <param name="tablename">Name of the table</param>
        /// <param name="partitionName">Name of the partition</param>
        /// <param name="dataOnly">flag to indicate, if only data needs to be refereshed</param>
        public void ProcessPartition(string tableName, string partitionName, bool dataOnly = false)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(tableName);

                    if (tableToProcess != null)
                    {
                        Partition partitionToProcess = tableToProcess.Partitions.Find(partitionName);
                        if (partitionToProcess != null)
                        {
                            partitionToProcess.RequestRefresh(dataOnly ? RefreshType.DataOnly : RefreshType.Full);
                           ModelSaveWithRetries(model);
                        }
                    }
                }
                finally { analysisServer.Disconnect(); }
            }

        }


        /// <summary>
        /// Process the specified partitions for the specified table
        /// </summary>
        /// <param name="tablename">Name of the table</param>
        /// <param name="partitions">Array of partition names</param>
        /// <param name="dataOnly">flag to indicate, if only data needs to be refereshed</param>
        public void ProcessPartitions(string tableName, string[] partitions, bool dataOnly = false)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(tableName);

                    if (tableToProcess != null)
                    {
                        foreach (string partitionName in partitions)
                        {
                            Partition partitionToProcess = tableToProcess.Partitions.Find(partitionName);
                            if (partitionToProcess != null)
                            {
                                partitionToProcess.RequestRefresh(dataOnly ? RefreshType.DataOnly : RefreshType.Full);
                            }

                           ModelSaveWithRetries(model);
                        }
                    }
                }
                finally { analysisServer.Disconnect(); }
            }

        }


        /// <summary>
        /// Creates artitions based on the information provided in the PartitionInfo list
        /// PartitionInfo specifies the name and the Source Query attributes of the partition.
        /// </summary>
        /// <param name="tableName">Name of the tabletableparam>
        /// <param name="partitionInfoList">List of new Partition information</param>
        public void CreatePartitions(string tableName, NewPartitionInfo[] partitionInfoList)
        {
            if (!String.IsNullOrEmpty(tableName) && partitionInfoList?.Length > 0)
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(tableName);

                    if (tableToProcess != null)
                    {
                        Partition templatePartition = tableToProcess.Partitions.Find(tableName);

                        if (templatePartition != null)
                        {
                            foreach (NewPartitionInfo newPartitionInfo in partitionInfoList)
                            {
                                Partition existingPartition = tableToProcess.Partitions.Find(newPartitionInfo.PartitionName);
                                if (existingPartition == null)
                                {
                                    AddPartitionToTable(newPartitionInfo, tableToProcess, templatePartition);
                                }
                            }
                           ModelSaveWithRetries(model);
                        }
                    }
                }
                finally { analysisServer.Disconnect(); }

            }
        }



        /// <summary>
        /// Creates the partition based on the specified partition information
        /// </summary>
        /// <param name="newPartitionInfo">New parition information</param>
        /// <param name="replaceExistingPartition">flag to indicate, if the existing partition with same names should be removed, prior to creating a new one</param>
        public void CreateNewPartition(NewPartitionInfo newPartitionInfo, bool replaceExistingPartition = false)
        {
            if (newPartitionInfo != null)
            {
                Server analysisServer = new Server();
                try
                {
                    analysisServer.Connect(this.ConnectionString);
                    Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                    Model model = database.Model;

                    Table tableToProcess = model.Tables.Find(newPartitionInfo.TableName);

                    if (tableToProcess != null)
                    {
                        Partition templatePartition = tableToProcess.Partitions.Find(newPartitionInfo.TableName);

                        Partition existingPartition = tableToProcess.Partitions.Find(newPartitionInfo.PartitionName);

                        /* Delete existing partition, if configured to replace partition */
                        if (replaceExistingPartition)
                        {
                            DeletePartition(model, tableToProcess, existingPartition);
                            existingPartition = null;
                        }

                        /* if partition does not exist or has been removed */
                        if (existingPartition == null)
                        {
                            AddPartitionToTable(newPartitionInfo, tableToProcess, templatePartition);
                           ModelSaveWithRetries(model);
                        }
                    }
                }
                finally { analysisServer.Disconnect(); }
            }
        }


        /// <summary>
        /// Merge the list of partitions to the specified partition list
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="targetPartitionInfo">Target partition information</param>
        /// <param name="sourcePartitionNames">Names of source partitions that will be merged</param>

        public void MergeParitions(string tableName, NewPartitionInfo targetPartitionInfo, string[] sourcePartitionNames)
        {
            if (String.IsNullOrEmpty(tableName)
                 || targetPartitionInfo == null
                 || sourcePartitionNames == null
                 || sourcePartitionNames?.Length == 0)
            {
                return;
            }


            List<Partition> partitionsToMerge = new List<Partition>();

            Server analysisServer = new Server();
            try
            {
                analysisServer.Connect(this.ConnectionString);
                Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                Model model = database.Model;

                Table tableToProcess = model.Tables.Find(tableName);

                if (tableToProcess != null)
                {
                    // Get reference to the target partition based on partition name
                    Partition existingTargetPartition = tableToProcess.Partitions.Find(targetPartitionInfo.PartitionName);

                    // if target partition does not exist
                    if (existingTargetPartition == null)
                    {
                        // create the new partition and disconnect                   
                        CreateNewPartition(targetPartitionInfo);
                        analysisServer.Disconnect();

                        //Refresh the model with new connection
                        analysisServer.Connect(this.ConnectionString);
                        database = analysisServer.Databases.GetByName(this.DatabaseName);
                        model = database.Model;
                        tableToProcess = model.Tables.Find(tableName);

                        existingTargetPartition = tableToProcess.Partitions.Find(targetPartitionInfo.PartitionName);
                    }

                    // if target parition exists (or was created in the prior step)
                    if (existingTargetPartition != null)
                    {
                        // for the given partition names
                        foreach (string partitionName in sourcePartitionNames)
                        {
                            // check if the source partition exist
                            Partition sourcePartition = tableToProcess.Partitions.Find(partitionName);
                            if (sourcePartition != null)
                            {
                                // if source partition exists , add to partition to merge list
                                partitionsToMerge.Add(sourcePartition);
                            }
                        }

                        // if there is partition is the partition list
                        if (partitionsToMerge.Count > 0)
                        {
                            // request merge and update the model
                            existingTargetPartition.RequestMerge(partitionsToMerge);
                           ModelSaveWithRetries(model);
                        }
                    }

                }
            }
            finally { analysisServer.Disconnect(); }
        }


        /// <summary>
        /// Determines if the specified partition exists in the table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="partitionName"></param>
        /// <returns>true, if it exists, else false</returns>
        public bool DoesPartitionExist(string tableName, string partitionName)
        {
            bool partitionExists = false;

            Server analysisServer = new Server();
            try
            {
                analysisServer.Connect(this.ConnectionString);
                Database database = analysisServer.Databases.GetByName(this.DatabaseName);
                Model model = database.Model;

                Table tableToProcess = model.Tables.Find(tableName);

                if (tableToProcess != null)
                {
                    // Get reference to the target partition based on partition name
                    Partition existingTargetPartition = tableToProcess.Partitions.Find(partitionName);
                    partitionExists = (existingTargetPartition != null);                
                }
            }
            finally { analysisServer.Disconnect(); }
            
            return partitionExists;
        }

        /// <summary>
        /// Adds the new partition to specified table based on the template query
        /// </summary>
        /// <param name="newParitionInfo"></param>
        /// <param name="tableToProcess"></param>
        /// <param name="templatePartition"></param>
        private static void AddPartitionToTable(NewPartitionInfo newParitionInfo, Table tableToProcess, Partition templatePartition)
        {

            if (newParitionInfo == null || tableToProcess == null || templatePartition == null) { return; }

            Partition newPartition = new Partition();

            templatePartition.CopyTo(newPartition);
            newPartition.Name = newParitionInfo.PartitionName;

            switch (newPartition.Source)
            {
                case MPartitionSource mSource:
                    mSource.Expression = newParitionInfo.SourceQuery;
                    break;
                case QueryPartitionSource querySource:
                    querySource.Query = newParitionInfo.SourceQuery;
                    break;
            }

            tableToProcess.Partitions.Add(newPartition);
        }



        /// <summary>
        /// Delete Partition 
        /// </summary>
        /// <param name="model">Analysis Server model </param>
        /// <param name="tableToProcess">Table to process </param>
        /// <param name="partitionName">Partition to remove</param>
        private void DeletePartition(Model model, Table tableToProcess, Partition partition)
        {
            if (partition != null)
            {
               tableToProcess.Partitions.Remove(partition);
               ModelSaveWithRetries(model);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="model"></param>
        /// <param name="numberOfRetries"></param>
        /// <param name="waitDurationInSeconds"></param>
        private void ModelSaveWithRetries(Model model)
        {
            Exception lastFailingException = null;
            bool success = false;

            try
            {
                model.SaveChanges();
                success = true;
            }
            catch (Exception initialException)
            {
                lastFailingException = initialException;
                success = false;
                int retryCount = 0;
                while ((retryCount < this.NumberOfRetries) && (!success))
                {
                    try
                    {
                        model.SaveChanges();
                        success = true;
                    }
                    catch (Exception nextException)
                    {
                        lastFailingException = nextException;
                        success = false;
                        retryCount++;
                        System.Threading.Thread.Sleep(this.GetWaitTimeBetweenRetries(retryCount));
                    }
                }
            }

            if (!success) { throw lastFailingException; }

            return;

        }


        /// <summary>
        /// Returns wait time between retries based on choosen wait pattern
        /// </summary>
        /// <param name="retryCount">Retry attempt count </param>
        /// <returns>Wait time in seconds </returns>
        private int GetWaitTimeBetweenRetries(int retryCount)
        {
            // default to 30 seconds, if retry interval not specified
            int waitTime = this.WaitTimeInSecondsBetweenRetries > 0 ?
                            this.WaitTimeInSecondsBetweenRetries : 30;

            switch (this.WaitPattern)
            {
                case RetryWaitPattern.Equal:
                    waitTime = waitTime * 1;
                    break;
                case RetryWaitPattern.Progressive:
                    waitTime = waitTime * retryCount;
                    break;
                case RetryWaitPattern.RandomlyProgressive:
                    waitTime = waitTime * (retryCount + new Random().Next(1, 11));
                    break;
            }

            return waitTime;
        }
    }
}