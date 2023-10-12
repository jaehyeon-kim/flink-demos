FROM flink:1.17.1

ARG PYTHON_VERSION
ENV PYTHON_VERSION=${PYTHON_VERSION:-3.8.10}
ARG FLINK_VERSION
ENV FLINK_VERSION=${FLINK_VERSION:-1.17.1}

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/$FLINK_VERSION/flink-connector-kafka-$FLINK_VERSION.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/$FLINK_VERSION/flink-sql-connector-kafka-$FLINK_VERSION.jar; \
  wget -P /opt/flink/lib/ https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar;

## Python version (3.7, 3.8, 3.9 or 3.10) is required, apt repo 
# Python 3.3 and later versions provide the lzma module. 
#   However, if Python is installed using the source code and the lzma-dev package is not installed in the system, 
#     the lzma module will not be installed.
# https://support.huawei.com/enterprise/en/doc/EDOC1100289998/db0db8f0/modulenotfounderror-no-module-named-_lzma-
# INFO:root:Starting up Python harness in a standalone process.
# Traceback (most recent call last):
#   File "/usr/local/lib/python3.8/site-packages/fastavro/read.py", line 2, in <module>
#     from . import _read
#   File "fastavro/_read.pyx", line 11, in init fastavro._read
#   File "/usr/local/lib/python3.8/lzma.py", line 27, in <module>
#     from _lzma import *
# ModuleNotFoundError: No module named '_lzma'

RUN apt-get update -y && \
  apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
  wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
  tar -xvf Python-${PYTHON_VERSION}.tgz && \
  cd Python-${PYTHON_VERSION} && \
  ./configure --without-tests --enable-shared && \
  make -j6 && \
  make install && \
  ldconfig /usr/local/lib && \
  cd .. && rm -f Python-${PYTHON_VERSION}.tgz && rm -rf Python-${PYTHON_VERSION} && \
  ln -s /usr/local/bin/python3 /usr/local/bin/python && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# install PyFlink
RUN pip3 install apache-flink==${FLINK_VERSION}