#!/bin/bash

dest="$(pwd)/resources/opt/bootstrap/"
GOPATH="$(pwd)"
cd src/alluxio.com
env GOOS=linux GOARCH=386 go build -o alluxio-ami-bootstrap ./main.go
chmod u+x alluxio-ami-bootstrap
mkdir -p ${dest}
mv alluxio-ami-bootstrap ${dest}
