namespace Microsoft.SqlServerAnaylsisServerTabularProcessing.Models
{
    /// <summary>
    /// Data class containing information about Tabular Model Table partitions
    /// </summary>
    public class NewPartitionInfo
    {
        /// <summary>
        /// Name of the table where the partition exists
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Name of the partition
        /// </summary>
        public string PartitionName { get; set; }

        /// <summary>
        /// Source query for the partition
        /// </summary>
        public string SourceQuery { get; set; }
    }
}
