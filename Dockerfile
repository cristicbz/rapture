FROM ubuntu:14.04

MAINTAINER Cristi Cobzarenco <cristi@reinfer.io>

RUN apt-get update
RUN apt-get install -y ssh tar
RUN apt-get install -y python python-pip
RUN apt-get install -y redis-server
RUN apt-get install -y python-dev
RUN pip install gevent
RUN pip install redis

ADD . /src
RUN python /src/test/test_rapture.py
