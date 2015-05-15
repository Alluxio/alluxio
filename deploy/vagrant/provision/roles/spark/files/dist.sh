#!/usr/bin/env bash

tar czf /tmp/spark.tar -C /spark/dist .
rm -rf /spark/*
tar xzf /tmp/spark.tar -C /spark
rm -f /tmp/spark.tar
