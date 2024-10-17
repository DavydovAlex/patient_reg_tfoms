FROM python:3.12.6-slim
WORKDIR /app
COPY requirements.txt .

ARG CLIENT=instantclient-basiclite-linux.x64-21.9.0.0.0dbru.zip

RUN grep -v pkg_resources requirements.txt > req_tmp.txt
RUN cat req_tmp.txt > requirements.txt; rm req_tmp.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN apt-get update && apt-get -yq install unzip
RUN apt-get install -y libaio1

COPY $CLIENT .
RUN unzip $CLIENT
RUN mkdir -p /opt/oracle/instantclient_21_9
RUN mv instantclient_21_9 /opt/oracle

