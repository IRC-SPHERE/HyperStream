![HyperStream logo](https://cdn.rawgit.com/IRC-SPHERE/HyperStream/dfbac332/hyperstream.svg)



# HyperStream #

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.242227.svg)](https://doi.org/10.5281/zenodo.242227)
[![Join the chat at https://gitter.im/IRC-SPHERE-HyperStream/Lobby](https://badges.gitter.im/IRC-SPHERE-HyperStream/Lobby.svg)](https://gitter.im/IRC-SPHERE-HyperStream/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/IRC-SPHERE/HyperStream.svg?branch=master)](https://travis-ci.org/IRC-SPHERE/HyperStream)
[![Dependency Status](https://www.versioneye.com/user/projects/58e423cb26a5bb005220301e/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/58e423cb26a5bb005220301e)
[![Test Coverage](https://codeclimate.com/github/codeclimate/codeclimate/badges/coverage.svg)](https://codeclimate.com/github/IRC-SPHERE/HyperStream/codeclimate/coverage)
[![Issue Count](https://codeclimate.com/github/codeclimate/codeclimate/badges/issue_count.svg)](https://codeclimate.com/github/IRC-SPHERE/HyperStream/codeclimate)

Hyperstream is a large-scale, flexible and robust software package for processing streaming data.

Hyperstream overcomes the limitations of other computational engines and provides high-level interfaces to execute complex nesting, fusion, and prediction both in online and offline forms in streaming environments. Although developed specifically for SPHERE, Hyperstream is a general purpose tool that is well-suited for the design, development, and deployment of algorithms and predictive models in a wide space of sequential predictive problems.

This software has been designed from the outset to be domain-independent, in order to provide maximum value to the wider community. Key aspects of the software include the capability to create complex interlinked workflows, and a computational engine that is designed to be "compute-on-request", meaning that no unnecessary resources are used. 

```diff
- NOTE: This software is in a stable but early beta, and hence the API may change significantly.
```

The system consists of the following 3 layers, from bottom up:

## Tools ##
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

## Streams ##
Streams are objects that use a particular kernel for computation, with fixed parameters and filters defined that can 
reduce the amount of data that needs to be read from the database. The stream is physically manifested in the database 
(mongodb) for the time ranges that it has been computed on.

There are special data streams, for which a custom hyperstream.interface.Input or hyperstream.interface.Output objects 
can be defined, in order to work with custom databases or file-based storage.

## Workflows ##
Workflows define a graph of streams. Usually, the first stream will be a special "raw" stream that pulls in data from a 
custom data source. Workflows can have multiple time ranges, which will cause the streams to be computed on all of the 
ranges given.

# Installation #

``` Bash
git clone git@github.com:IRC-SPHERE/HyperStream.git
cd HyperStream
virtuenv venv
. venv/bin/activate
pip install -r requirements.txt
python -c 'from hyperstream import HyperStream'
```

# Running tests #

Run the following command
```
nosetests
```

Note that for the MQTT logging test to succeed, you will need to have an MQTT broker running (e.g. Mosquitto). For example:

```
docker run -ti -p 1883:1883 -p 9001:9001 toke/mosquitto
```

or on OSX you will need pidof and mosquitto:

```
brew install pidof
brew install mosquitto
brew services start mosquitto
```



# Running the examples #

```diff
- TODO
```

# License #

This code is released under the [MIT license](https://github.com/IRC-SPHERE/Infer.NET-helpers/blob/master/LICENSE).

# Acknowledgements #

This work has been funded by the UK Engineering and Physical Sciences Research Council (EPSRC) under Grant [EP/K031910/1](http://gow.epsrc.ac.uk/NGBOViewGrant.aspx?GrantRef=EP/K031910/1) -  "SPHERE Interdisciplinary Research Collaboration".

