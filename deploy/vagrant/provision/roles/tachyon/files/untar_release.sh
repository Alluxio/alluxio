#!/usr/bin/env bash

DIST=/vagrant/shared/$TACHYON_DIST
tar xzf $DIST -C /tachyon --strip-components 1
