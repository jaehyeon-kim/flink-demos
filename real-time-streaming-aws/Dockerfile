FROM flink:1.17.1

ARG PYTHON_VERSION
ENV PYTHON_VERSION=${PYTHON_VERSION:-3.8.10}
ARG FLINK_VERSION
ENV FLINK_VERSION=${FLINK_VERSION:-1.17.1}

RUN mkdir ./plugins/s3-fs-hadoop \
  && cp ./opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar ./plugins/s3-fs-hadoop 

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

# add kafka client for Flink SQL client, will be added manually
RUN wget -P /etc/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar;
