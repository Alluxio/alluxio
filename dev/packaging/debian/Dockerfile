FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    debhelper

COPY package.sh /

ENTRYPOINT [ "/package.sh" ]