# Azure Function App for Azure Analysis Services Tabular Model Operations

This project allows the client to perform common operations on a Analysis Services tabular model database hosted on a Azure Analysis Services instance through an Azure Function App.

 The Azure Function App exposes HTTP endpoints to perform these common operations. Each Azure function deployment is associated (via application settinsg) with a single Azure Analysis Services instance. The operations can be performed in any tabular model hosted on that instance.
 
 The code is intended to support on-demand tabular model processing, in a data processing pipeline. The operations include:
- Creating of partitons
- Processing of partitions, table and model in a synchronous or asynchornous manner
- Merging on paritions
- Deleting partitions

 
Using the base libraries, it is possible to add more functionality to perform combination of operation in a single request. 
For example:
- Creation of daily and monthly parititions for large tables
- Merging daily partitions in to monthlty partitions.

The scenarios, where this code is likely to be used will be integrating the process with third party data orchestration engine such as Informatica, which can hit HTTP endpoints to trigger operations.

I encourage you to review the [Application Design](./docs/AppDesign.md) for more details such as external dependencies and implementation details, to see how the code can be adapted for your situation. 

If the scenario just requires tabular model refreshes, an alternative way is available [here](https://docs.microsoft.com/en-us/azure/analysis-services/analysis-services-async-refresh). 


## Deployment Model
The following components are required:
- Azure Analysis Services Instance
- Function App (Version 1) (requires Full .NET Framework 4.6.1 or higher on Microsoft Windows host)