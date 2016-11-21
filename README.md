# HyperStream #

Software to control all of the data processing workflows (streams) for deployment and experimentation.

## Deployment ##

For deployment instructions see [the deployment docs](deployment.md)




The system consists of the following 3 layers, from bottom up:

# Tools #
Tools are the computation elements. They take in input data in a standard format (dict of list of 
hyperstream.instance.Instance objects) and output data in a standard format (list of 
hyperstream.instance.Instance objects). Tools are version controlled. Minor version numbers should be used for updates
 that will not require recomputing streams, since the output should be identical (in expectation for stochastic 
 streams). Major version number changes will cause the stream to be recomputed.

### Tool Versions ###
The tool versions form a major/minor/patch 3-tuple, e.g. v1.3.2. See https://pypi.python.org/pypi/semantic_version/ for details.
In our setting the major version number is treated as a binary flag: 0 for development, 1 for production. Minor version 
numbers indicate changes that affect the output, or in the API. The patch number indicates changes that do not affect the 
output or API in any way (e.g. speedups).

# Streams #
Streams are objects that use a particular kernel for computation, with fixed parameters and filters defined that can 
reduce the amount of data that needs to be read from the database. The stream is physically manifested in the database 
(mongodb) for the time ranges that it has been computed on. 

There are special data streams, for which a custom hyperstream.interface.Input or hyperstream.interface.Output objects 
can be defined, in order to work with custom databases or file-based storage.

# Workflows #
Workflows define a graph of streams. Usually, the first stream will be a special "raw" stream that pulls in data from a 
custom data source. Workflows can have multiple time ranges, which will cause the streams to be computed on all of the 
ranges given.
