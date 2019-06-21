#!/usr/bin/env bash

mkdir -p /alluxio/assembly/target
rsync -avz AlluxioMaster:/alluxio/* /alluxio
