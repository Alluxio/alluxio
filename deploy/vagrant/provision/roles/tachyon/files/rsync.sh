#!/usr/bin/env bash

mkdir -p /tachyon/core/target
rsync -avz TachyonMaster.local:/tachyon/assembly/target/tachyon-assmeblies-*-jar-with-dependencies.jar /tachyon/core/target
rsync -avz TachyonMaster.local:'/tachyon/bin /tachyon/conf /tachyon/libexec' /tachyon
