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

FROM maven:3.5.4-jdk-8

# Add jenkins user to make it easier to use this image from jenkins contexts
RUN adduser --quiet jenkins
RUN usermod -u 1000 jenkins && groupmod -g 1000 jenkins

RUN apt-get update -y && \
    apt-get install -y golang-go ruby ruby-dev make build-essential && \
    gem install jekyll bundler

# Give jenkins user access to /root/.m2. We can still run the image as root for local debugging
RUN mkdir -p /root/.m2 && \
    chown -R jenkins:jenkins /root/.m2
