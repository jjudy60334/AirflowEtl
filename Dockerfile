#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM apache/airflow:2.1.4-python3.7
SHELL ["/bin/bash", "-xc"]
USER root
RUN apt-get update 
RUN apt-get install -y vim 
RUN apt-get install -y git
RUN curl -sSL https://get.docker.com/ | sh
RUN usermod -aG docker airflow
COPY requirements.txt /

# RUN adduser --disabled-password --gecos '' airflow
# RUN  apt-get update \
#     && apt-get -y install build-essential\
#     && apt -y install  default-libmysqlclient-dev
# ARG AIRFLOW_DEPS="async_packages,atlas,aws,celery,cgroups,cloudant,dask,doc,docker,flask_oauth,grpc,kubernetes,google_auth,mysql,odbc,papermill,password,postgres,redis,sentry,slack,ssh,virtualenv,devel"

# RUN pip install --no-cache-dir  -e .[$AIRFLOW_DEPS] 
RUN chown -R airflow /var/lib/dpkg
ENV AIRFLOW_HOME=/opt/airflow
# ENV HOME=/opt/airflow
USER airflow
RUN pip install --upgrade pip
# RUN pip install --upgrade install pip==20.2.4
RUN pip install --ignore-installed -r /requirements.txt

WORKDIR $AIRFLOW_HOME
