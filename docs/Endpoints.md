# HTTP Endpoints Description

| azure function name | HTTP Operation | endpoints | path parameters | body | description |
| --- | --- | --- | --- | --- | --- | ---- |
| ProcessModel | GET | `<host>://api/ProcessTabularModel/{databaseName}` | databaseName: Analysis service database| none | Process the full database model. Processes all the tables in the specified database.|
| ProcessTable | GET |`<host>://api/ProcessTabularModel/{databaseName}/{tableName}`| databaseName: Analysis service database hosting the table. tableName: Table to process | none | Process the table hosted in the specified analysis services dataset. Processes all the existing partitions in the specified table.|
| ProcessPartition | GET |`<host>://api/ProcessTabularModel/{databaseName}/tables/{tableName}/partitions/{partitionName}`|databaseName: Analysis service database hosting the table.  tableName: Table containing the partition.  partitionName: Name of the partition to process|none|Process the specified partition in the specified table and database.|
| ProcessModelAsync | GET | `<host>://api/ProcessTabularModel/{databaseName}/async` | databaseName: Analysis service database| none | Queues the request to process the specified database.|
| ProcessTableAsync | GET |`<host>://api/ProcessTabularModel/{databaseName}/{tableName}/async`| databaseName: Analysis service database hosting the table. tableName: Table to process | none | Queues a request to process the specified table in the specified database. |
| ProcessPartition | GET |`<host>://api/ProcessTabularModel/{databaseName}/tables/{tableName}/partitions/{partitionName}/async`|databaseName: Analysis service database hosting the table.  tableName: Table containing the partition.  partitionName: Name of the partition to process|none|Queues a request to process the specified partition in the specified table and database.|

