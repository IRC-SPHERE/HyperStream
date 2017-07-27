![HyperStream logo](https://cdn.rawgit.com/IRC-SPHERE/HyperStream/dfbac332/hyperstream.svg)



# HyperStream #

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.242227.svg)](https://doi.org/10.5281/zenodo.242227)
[![Join the chat at https://gitter.im/IRC-SPHERE-HyperStream/Lobby](https://badges.gitter.im/IRC-SPHERE-HyperStream/Lobby.svg)](https://gitter.im/IRC-SPHERE-HyperStream/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/IRC-SPHERE/HyperStream.svg?branch=master)](https://travis-ci.org/IRC-SPHERE/HyperStream)
[![Dependency Status](https://www.versioneye.com/user/projects/58e423cb26a5bb005220301e/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/58e423cb26a5bb005220301e)
[![Test Coverage](https://codeclimate.com/github/IRC-SPHERE/HyperStream/badges/coverage.svg)](https://codeclimate.com/github/IRC-SPHERE/HyperStream/coverage)
[![Issue Count](https://codeclimate.com/github/IRC-SPHERE/HyperStream/badges/issue_count.svg)](https://codeclimate.com/github/IRC-SPHERE/HyperStream)
[![Documentation Status](https://readthedocs.org/projects/hyperstream/badge/?version=latest)](http://hyperstream.readthedocs.io/en/latest/?badge=latest)

Hyperstream is a large-scale, flexible and robust software package for processing streaming data.

* HyperStream [homepage](https://irc-sphere.github.io/HyperStream/)
* Tutorial [notebooks](http://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/)
* Gitter [chat room](https://gitter.im/IRC-SPHERE-HyperStream/Lobby)
* Developer [documentation](http://hyperstream.readthedocs.io/en/latest/)

Hyperstream overcomes the limitations of other computational engines and provides high-level interfaces to execute complex nesting, fusion, and prediction both in online and offline forms in streaming environments. Although developed specifically for SPHERE, Hyperstream is a general purpose tool that is well-suited for the design, development, and deployment of algorithms and predictive models in a wide space of sequential predictive problems.

This software has been designed from the outset to be domain-independent, in order to provide maximum value to the wider community. Key aspects of the software include the capability to create complex interlinked workflows, and a computational engine that is designed to be "compute-on-request", meaning that no unnecessary resources are used. 

# Installation #
## Docker images ##
If you do not want to install all the packages separately you can use our Docker bundle available [here](https://github.com/IRC-SPHERE/Hyperstream-Dockerfiles).

## Local machine ##
Install via pip

```
pip install hyperstream
python -c 'from hyperstream import HyperStream'
```

To get the latest version

```
pip install -U git+git://github.com/IRC-SPHERE/HyperStream.git#egg=hyperstream
pip install -r requirements.txt
```

Or clone the repository

``` Bash
git clone git@github.com:IRC-SPHERE/HyperStream.git
cd HyperStream
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
python -c 'from hyperstream import HyperStream'
```

Additionally, one of the requirements to run Hyperstream is a MongoDB server. By default, Hyperstream tries to connect to the port 27017 on the localhost.

To see the installation steps of MongoDB go to the [official documentation][1]. E.g. in a Debian OS it is possible to install with the following command

``` Bash
sudo apt-get install mongodb
```

Once the MongoDB server is installed, it can be started with the following command

``` Bash
service mongod start
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

# Tutorials #

The following tutorials show how to use HyperStream in a step-by-step guide.

- [Tutorial 1: Introduction][2]
- [Tutorial 2: Creating tools][3]
- [Tutorial 3: Stream composition][4]
- [Tutorial 4: Real-time streams][5]
- [Tutorial 5: Workflows][6]

It is possible to run all the tutorials in your own machine ussing Docker containers defined in [IRC-SPHERE/Hyperstream-Dockerfiles](https://github.com/IRC-SPHERE/HyperStream-Dockerfiles). You can do that by running the following commands:

```bash
git clone https://github.com/IRC-SPHERE/Hyperstream-Dockerfiles.git
cd Hyperstream-Dockerfiles
docker-compose -f docker-compose-tutorials.yml -p hyperstream-tutorials up
```

And then open the url http://0.0.0.0:8888/tree in a web-browser

## Simple use-case #

```Python
from hyperstream import HyperStream, StreamId, TimeInterval
from hyperstream.utils import utcnow, UTC
from datetime import timedelta

hs = HyperStream(loglevel=20)
M = hs.channel_manager.memory
T = hs.channel_manager.tools
clock = StreamId(name="clock")
clock_tool = T[clock].window().last().value()
ticker = M.get_or_create_stream(stream_id=StreamId(name="ticker"))
now = utcnow()
before = (now - timedelta(seconds=30)).replace(tzinfo=UTC)
ti = TimeInterval(before, now)
clock_tool.execute(sources=[], sink=ticker, interval=ti, alignment_stream=None)
list(ticker.window().tail(5))
```

The last list contains

```Python
[StreamInstance(timestamp=datetime.datetime(2017, 7, 27, 10, 33, 45, tzinfo=<UTC>), value=datetime.datetime(2017, 7, 27, 10, 33, 45, tzinfo=<UTC>)),
 StreamInstance(timestamp=datetime.datetime(2017, 7, 27, 10, 33, 46, tzinfo=<UTC>), value=datetime.datetime(2017, 7, 27, 10, 33, 46, tzinfo=<UTC>)),
 StreamInstance(timestamp=datetime.datetime(2017, 7, 27, 10, 33, 47, tzinfo=<UTC>), value=datetime.datetime(2017, 7, 27, 10, 33, 47, tzinfo=<UTC>)),
 StreamInstance(timestamp=datetime.datetime(2017, 7, 27, 10, 33, 48, tzinfo=<UTC>), value=datetime.datetime(2017, 7, 27, 10, 33, 48, tzinfo=<UTC>)),
 StreamInstance(timestamp=datetime.datetime(2017, 7, 27, 10, 33, 49, tzinfo=<UTC>), value=datetime.datetime(2017, 7, 27, 10, 33, 49, tzinfo=<UTC>))]
```

# HyperStream Viewer #
The [HyperStream Viewer](https://github.com/IRC-SPHERE/HyperStreamViewer) is a python/Flask web-app for interacting with HyperStream. In order to keep HyperStream to a minimum, this web-app is released as a separate repository that takes the core as a dependency.

# License #

This code is released under the [MIT license](https://github.com/IRC-SPHERE/HyperStream/blob/master/LICENSE).

# Acknowledgements #

This work has been funded by the UK Engineering and Physical Sciences Research Council (EPSRC) under Grant [EP/K031910/1](http://gow.epsrc.ac.uk/NGBOViewGrant.aspx?GrantRef=EP/K031910/1) -  "SPHERE Interdisciplinary Research Collaboration".


[1]: https://docs.mongodb.com/manual/installation/

[2]: https://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/tutorial_01.ipynb
[3]: https://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/tutorial_02.ipynb
[4]: https://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/tutorial_03.ipynb
[5]: https://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/tutorial_04.ipynb
[6]: https://nbviewer.jupyter.org/github/IRC-SPHERE/HyperStream/blob/master/examples/tutorial_05.ipynb
