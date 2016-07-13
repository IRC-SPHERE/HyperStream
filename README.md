# HyperStream #

Software to control all of the data processing workflows (streams) for deployment and experimentation.



The system consists of the following 3 layers, from bottom up:

# Kernels #
Kernels are the computation elements. They take in input data in a standard format (dict of list of 
hyperstream.instance.Instance objects) and output data in a standard format (list of 
hyperstream.instance.Instance objects). Kernels are version controlled. Minor version numbers should be used for updates
 that will not require recomputing streams, since the output should be identical (in expectation for stochastic 
 streams). Major version number changes will cause the stream to be recomputed.

# Streams #
Streams are objects that use a particular kernel for computation, with fixed parameters and filters defined that can 
reduce the amount of data that needs to be read from the database. The stream is physically manifested in the database 
(mongodb) for the time ranges that it has been computed on. 

There are special data streams, for which a custom hyperstream.interface.Input or hyperstream.interface.Output objects 
can be defined, in order to work with custom databases or file-based storage.

# Flows #
Flows define a graph of streams. Usually, the first stream will be a special "raw" stream that pulls in data from a 
custom data source. Flows can have multiple time ranges, which will cause the streams to be computed on all of the 
ranges given.
