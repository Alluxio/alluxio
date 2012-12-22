#!/bin/bash

rm -rf ../main/java/tachyon/thrift

thrift --gen java -out ../main/java/.  tachyon.thrift
