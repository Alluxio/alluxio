#!/usr/bin/env bash

mkdir -p /tachyon/assembly/target
rsync -avz TachyonMaster:/tachyon/* /tachyon
