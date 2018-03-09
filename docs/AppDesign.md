# Azure Functions App Design for AAS Tabular Database Operations

## Implementation Details

### Development tools

#### Visual Studio 2017 - IDE

The code was developed using Visual Studio 2017 v15.4 in conjunction with Azure Portal used to provision the container for Azure functions.

#### Azure Functions and Web Jobs Tools

The Azure Function code depends upon - Azure Functions and Web Jobs Tools, installed as an extension to Visual Studio 2017. This provides the SDK and facilitates local debugging of Azure functions prior to deployment to the Azure cloud.

### External Dependencies
This code has an external dependencies on the following libraries:

SQL Server Analysis Services - Analysis Service Management Objects (AMO). The library can be downloaded using the following link - https://docs.microsoft.com/en-us/sql/analysis-services/instances/install-windows/install-analysis-services-data-providers-amo-adomd-net-msolap


### Code Organization

#### Visual Studio Solution and Projects
The following section describes the projects included in the Tabular Model Ops solution:

![alt text][slnAASTabularModelOps]

The Azure Functions application is divided into two projects:

* **AzFunctionApp** : Azure functions code, containing the endpoints to process the Analysis Services Tabular Model. It can additionally host any business logic/flow for Tabular model operations.

* **SqlServerAnalysisServerTabularProcessing** : .NET Library, wrapping around the Analysis Service Management Objects library to simplify processing and partition management, such as creation and merging


#### Application Flow - Synchronous Mode

The application functionality is exposed to the client or the orchestration tool such as the Informatica as HTTP endpoints.

The HTTP requests to the Azure functions can carry the following information, depending on the type of operations, the HTTP query or the body:
- Database Name (all operations)
- Table Name (most operations)
- Partition Name (some operations)
- Source Query (some operations)

Based on the these parameters, the Azure function is able to make calls to the appropriate database hosting the tabular model. These are blocking HTTP calls and can last between 30 seconds to several minutes (upto 2-3 minutes) depending on the size of the data to be processed. Successful completion return HTTP 200 response to the user with informational message. Errors are propogate using HTTP 500 error response with exception information.

**IMPORTANT**: While hosting the Function App in Azure, the synchronous operations built using HTTP trigger are also subject to by 230 seconds (little less than 4 minutes) idle connection timeout on Azure Load Balancer. This is a hard limit and is not user configurable. Even though the operation continues to run in the background and will complete successfully, the client will receive a HTTP 500 error indicating a time our from the load balancer at end of timeout limit.

Given that limitation, while calling the synchornous operations ensure they do not  last more than 230 seconds. The operation that fit into this category would be creating new paritions, merging paritions or processing smaller tables (dimension) and partitions.

#### Application Flow - Asynchronous Mode

In order to avoid the time out error, we support asynchornous way to invoking some of the operations. 

The general pattern of invoking the operation is fairly standard. Call an endpoint to queue the request and receive a tracking information. Using the tracking information, query on the progress of the background operation using another endpoint on intervals,(Recommended interval: 30 - 60 seconds), till the status indicates a successful or unsuccessful completion.

![alt text][asyncdesign]

To support the asynchronous functionilty there are two new Azure function introduced for each kind of processing request. Taking the example of processing a table asynchronously. We have the following Azure Functions classes:

*ProcessTabularModelProcessTableAsync*: Exposes a HTTP endpoint to accept request to process the table in the specified database. This request is placed in a table processing request queue and returns the tracking information to the client. The tracking information is returned using HTTP 202 (Accepted) status code.

*ProcessTabularModelDequeueProcessTable*: Triggered by a entry in the table processing request queue. Dequeues the request and processes the tables and provides intermediary updates. These function run independently of each other using Azure Storage - Tables and Queues to communicate.

Azure Table and Queue for each of the operation is configured in the Application settings.

Finally the class - *ProcessTabularModelGetProcessingStatus* provides HTTP endpoint which the client can use to query the status of the cube processing request using the tracking information.The request to this endpoint can return - HTTP 200 (if tracking record is found), HTTP 404 (if tracking record is not found) and HTTP 400 (if invalid parameters are sent).

For example:

To request asynchornous processing of the "Suppliers" dimensions table in AdventureWorks sample anaysis services database, the request would look like as follows:

```
https://azfunctionendpoint.azurewebsites.net/api/ProcessTabularModel/adventureworks/tables/Suppliers/async?code=key
```

The endpoint returns the following response to indicate the request has been queued:

```
{
    "LaunchDateKey": "2018-01-25"
    "TrackingId": "6454bae6-d61e-4ec3-bd74-313d80876f6e",,
    "Status": "Queued",
}
```

The tracking information is embedded in the two field names - LaunchDateKey and TrackingId. The query for the monitoring endpoint becomes

```
https://azfunctionendpoint.azurewebsites.net/api/ProcessTabularModel/processing/status/table/2018-01-25/6454bae6-d61e-4ec3-bd74-313d80876f6e?code=key
```

Sample response may look as follows:

```
{
    "LaunchDateKey": "2018-01-25"
    "TrackingId": "6454bae6-d61e-4ec3-bd74-313d80876f6e",,
    "Status": "Running",
}
```


The status field indicates progress in processing and the valid values for the Status are as follows:
- Queued
- Running
- Error Processing
- Complete.
 
 *Error Processing* and *Complete* are the two end states.


### Application Settings

The database and connectivity information is stored external to the Azure functions application. Currently each deployment of Azure Function can support only one database server per deployment instance. This configuration is stored in Azure Functions Application Settings as Connection String with the key -  **_"SsasTabularConnection"_** .

To support the Asynchornous processing, there are additional six settings:

- **ProcessModelQueue**: Name of the queue to request Model Processing.
- **ProcessModelStatusTable**: Table to hold status of requests for Model Processing.
- **ProcessPartitionQueue**: Name of the queue to request partition processing
- **ProcessPartitionStatusTable**: Table to hold status of requests for partition processing.
- **ProcessTableQueue**: Name of the queue to request  table processing.
- **ProcessTableStatusTable**: Table to hold status of requests for table processing.

Sometimes the processing of model, partitions and tables fail, due to transient reason such as database locks. The code supports a configuration option to retry the processing when such errors happen. 

The settings for the retry operations are as follows:

- **MaximumRetries**: An integer that defines the maximum number for times, the application will retry the failed operation. Retries are enabled when value is greater than zero.   Minimum Value: 0 (no retries). Recommended Value: 3
- **WaitTimeInSeconds**: An integer that defines  the wait time between retries in seconds. Default: 30 seconds, if retries are enabled. (MaximumRetries > 0)
- **RetryWaitPattern** : Selection of values that define pattern the wait time between retries. Valid Values: 
   - _Equal_ : Same amount of time between retries.
   - _Progressive_ : Increase the amount of time between the retries progressively. Example: First retry after 30 seconds, second retry - 2 * 30 seconds = 60 seconds, third retry - 3* 30 = 90 seconds and so on.
   - _ProgressiveRandom_: Adds a random amount of time between 1 to 10 seconds to progressive wait time.


### **Developer Notes**
On the development machine, it is stored in the file - "local.settings.json". This file is not checked into the repository due to nature of the content (secrets like connection string, keys). Howerver, the repository has a sample settings file called - "sample.settings.json". While developing/debugging, rename this file to - "local.settings.json" and your developer environment specific configuration values.

## Extending the code

### Parition Creation Support
If there is a need to create partitions through the Azure functions application for monthly or daily basis or create inital monthly paritions with repartition endpoint, there are helper functions available.

To create partitions on monthly and daily basis for tables, the application needs to configured with the a source query string for each table with higher and lower bound, accepted as substitutable parameters. 

An example included in the code for the table - "FactInternetSales" from AdventureWorks Analysis Services Database. To add additional table or modify the query for existing tables, current code modification and redeployment of Azure Functions is required.

The code to be modified is in the Azure Functions application code - /Utility/QueryHelper.cs

![alt text][queryhelperCsTabularModelOps]

Sample code in QueryHelper.cs

![alt text][queryhelperCsCodeSnippetTabularModelOps]

The current code shows the source query for the metrics-internet-sales table, with start and end date to substituted for each partition.

### Business Logic Support
The business logic code for cube processing is hosted in Azure function code and within each endpoints. Additional business logic or modifications are recommended to be made at this layer or into a separate library.

### Other References
The application code for this application is heavily influenced by the following Open Source Project hosted on GitHub, provided by SQL Server Analysis Services Product Team,

https://github.com/Microsoft/Analysis-Services/tree/master/AsPartitionProcessing


[slnAASTabularModelOps]: ./images/AASTabularModelOps_VS_Soln.png "Visual Studio Solution - AAS Tabular Model Ops"

[queryhelperCsTabularModelOps]:./images/AASTabularModelOps_VS_QueryHelper_cs.png "Visual Studio - QueryHelper.cs in Solution Explorer"

[queryhelperCsCodeSnippetTabularModelOps]:./images/AASTabularModelOps_VS_QueryHelper_cs_CodeSnippet.png "Visual Studio - QueryHelper.cs - Code Snippet"

[asyncdesign]:./images/AASTabularModelOps_AsyncDesign.png "Tabular Model Operations - Asynchronous Design"

