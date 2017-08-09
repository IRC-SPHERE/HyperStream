FROM ubuntu:16.04

MAINTAINER Kacper Sokol <ks1591@bristol.ac.uk>

ARG DEBIAN_FRONTEND=noninteractive

USER root
RUN apt-get update \
  && apt-get upgrade -y

RUN apt-get install -y \
    git \
    python \
    python-pip


ENV SHELL /bin/bash
ENV HYPERSTREAM_USER jovyan
ENV HYPERSTREAM_UID 1000
ENV HOME /home/$HYPERSTREAM_USER
ENV HYPERSTREAM_DIR $HOME/hyperstream

# Create jovyan user with UID=1000 and in the 'users' group \ -N -u $SWISH_UID
RUN useradd -m -s $SHELL $HYPERSTREAM_USER

RUN mkdir -p $HYPERSTREAM_DIR \
  && chown $HYPERSTREAM_USER $HYPERSTREAM_DIR
USER $HYPERSTREAM_USER

RUN git clone --depth=1 https://github.com/IRC-SPHERE/HyperStream.git $HYPERSTREAM_DIR
RUN pip install --user -r $HYPERSTREAM_DIR/requirements.txt

ENV PATH $HOME/.local/bin:$PATH

WORKDIR $HYPERSTREAM_DIR

# Configure mongo connection
RUN sed -i -e 's/localhost/hyperstream-mongo/g' hyperstream_config.json

# Configure MQTT connection
RUN sed -i -e 's/mqtt_ip = "127.0.0.1"/mqtt_ip = "hyperstream-mqtt"/g' tests/helpers.py

# Test HyperStream
RUN python -c 'from hyperstream import HyperStream'

# TODO: Run hyperstream rather than test it on container bootup
ENTRYPOINT nosetests --with-coverage
#ENTRYPOINT /bin/bash
