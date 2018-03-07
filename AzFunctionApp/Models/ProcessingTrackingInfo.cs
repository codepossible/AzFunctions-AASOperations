namespace AzFunctionApp.Models
{
    /// <summary>
    /// Tracking information for processing launched asychnronously 
    /// </summary>
    public class ProcessingTrackingInfo
    {
        /// <summary>
        ///Parition Key for Azure Table Storage - Date when processing was lauched
        /// </summary>
        public string LaunchDateKey { get; set; }

        /// <summary>
        ///Row Key for Azure Table Storage - Unique tracking key (GUID) 
        /// </summary>
        public string TrackingId { get; set; }

        /// <summary>
        ///Status of the processing - Queued, Running, Completed or Error Processing 
        /// </summary>
        public string Status { get; set; }
    }
}
