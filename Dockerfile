FROM ubuntu:13.10

MAINTAINER Cristi Cobzarenco <cristi@reinfer.io>

RUN apt-get update
RUN apt-get install -y ssh tar
RUN apt-get install -y python python-pip
RUN apt-get install -y redis-server
RUN apt-get install -y python-dev
RUN pip install gevent
RUN pip install redis
RUN mkdir -p /src

VOLUME "/src"
CMD python src/test/test_rapture.py

