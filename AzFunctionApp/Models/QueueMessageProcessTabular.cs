using System;
using Microsoft.WindowsAzure.Storage.Table;


namespace AzFunctionApp.Models
{
    /// <summary>
    /// Information stored in Azure Table and Queue in Azure Queue
    /// for processing requests of tables and partitions
    /// </summary>
    public class QueueMessageProcesssTabular : TableEntity
    {
        /// <summary>
        /// Row Key for Azure Table Storage - Unique tracking key (GUID).
        /// </summary>
        public string TrackingId { get; set; }

        /// <summary>
        ///Date time when the request was made (stored as UTC).
        /// </summary>        
        public DateTime EnqueuedDateTime { get; set; }

        /// <summary>
        /// Database name where processing will take place.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// A table or list of comma seperated tables in the database where processing will take place.
        /// </summary>
        public string Tables { get; set; }

        /// <summary>
        /// The partition in the table in the database where processing will take place.
        /// </summary>
        public string Parition { get; set; }

        /// <summary>
        /// Date and time when the process started
        /// </summary>
        public DateTime TargetDate { get; set; }

        /// <summary>
        /// Status of the processing - Queued, Running, Completed or Error Processing
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// Error details if the status is Error processing
        /// </summary>
        public string ErrorDetails { get; set; }

        /// <summary>
        /// Conversion to Processing Tracking Info
        /// </summary>
        /// <returns>returns the tracking information</returns>
        public ProcessingTrackingInfo ToProcessingTrackingInfo()
        {
            return new ProcessingTrackingInfo()
            {
                LaunchDateKey = this.PartitionKey,
                TrackingId = this.RowKey,
                Status = this.Status
            };
        }

    }
}
