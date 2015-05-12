#!/usr/bin/env bash

mkdir -p /tachyon/core/target
rsync -avz TachyonMaster.local:/tachyon/core/target/tachyon-*-jar-with-dependencies.jar /tachyon/core/target
rsync -avz TachyonMaster.local:'/tachyon/bin /tachyon/conf /tachyon/libexec' /tachyon
