namespace Microsoft.SqlServerAnaylsisServerTabularProcessing.Models
{
    /// <summary>
    /// Data Class to perform the partition merging
    /// </summary>
    public class MergeParitionInfo
    {
        /// <summary>
        /// Table where the new partition is to be created
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Information about the target parition
        /// </summary>
        public NewPartitionInfo TargetPartition { get; set; }

        /// <summary>
        /// List of names of the source partitions to merge into the target partition
        /// </summary>
        public string[] SourcePartitionNames { get; set; }      
    }
}
