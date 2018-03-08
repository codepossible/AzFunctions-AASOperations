# Azure Analysis Services Tabular Model Processing of using Azure Function App

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

