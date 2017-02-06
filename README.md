![HyperStream logo](https://cdn.rawgit.com/IRC-SPHERE/HyperStream/dfbac332/hyperstream.svg)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.242227.svg)](https://doi.org/10.5281/zenodo.242227)

# HyperStream #

Hyperstream is a large-scale, flexible and robust software package for processing streaming data.

Hyperstream overcomes the limitations of other computational engines and provides high-level interfaces to execute complex nesting, fusion, and prediction both in online and offline forms in streaming environments. Although developed specifically for SPHERE, Hyperstream is a general purpose tool that is well-suited for the design, development, and deployment of algorithms and predictive models in a wide space of sequential predictive problems.

This software has been designed from the outset to be domain-independent, in order to provide maximum value to the wider community. Key aspects of the software include the capability to create complex interlinked workflows, and a computational engine that is designed to be "compute-on-request", meaning that no unnecessary resources are used. 

```diff
- NOTE: This software is in a stable but early beta, and hence the API may change significantly.
```

The system consists of the following 3 layers, from bottom up:

# Tools #
Tools are the computation elements. They take in input data in a standard format (dict of list of 
hyperstream.instance.Instance objects) and output data in a standard format (list of 
hyperstream.instance.Instance objects). Tools are version controlled. Minor version numbers should be used for updates
 that will not require recomputing streams, since the output should be identical (in expectation for stochastic 
 streams). The output should be identical (again, in expectation for stochastic streams) regardless of whether the tool is run twice on time-ranges t1..t2 and t2..t3 or just once on the time-range t1..t3. Major version number changes will cause the stream to be recomputed.

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

# Installation #

```diff
- TODO
```

# Running tests #

```diff
- TODO
```

# Running the examples #

```diff
- TODO
```

# License #

This code is released under the [MIT license](https://github.com/IRC-SPHERE/Infer.NET-helpers/blob/master/LICENSE).
