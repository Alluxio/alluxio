#!/usr/bin/env bash

mkdir -p /tachyon/assembly/target
rsync -avz TachyonMaster:/tachyon/assembly/target/tachyon-assemblies-*-jar-with-dependencies.jar /tachyon/assembly/target
rsync -avz TachyonMaster:'/tachyon/bin /tachyon/conf /tachyon/libexec' /tachyon
