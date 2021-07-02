#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

# See https://hub.docker.com/r/alluxio/alluxio-maven for instructions on running the image.

FROM maven:3.6.3-jdk-8

# need to create /.config to avoid npm errors
RUN mkdir -p /home/jenkins && \
    chmod -R 777 /home/jenkins && \
    chmod g+w /etc/passwd && \
    mkdir -p /.config && \
    chmod -R 777 /.config && \
    apt-get update -y && \
    apt-get install -y golang-go ruby ruby-dev make build-essential fuse && \
    gem install jekyll bundler
RUN wget --quiet https://releases.hashicorp.com/terraform/0.12.24/terraform_0.12.24_linux_amd64.zip && \
    unzip -o ./terraform_0.12.24_linux_amd64.zip -d /usr/local/bin/ && \
    rm terraform_0.12.24_linux_amd64.zip
