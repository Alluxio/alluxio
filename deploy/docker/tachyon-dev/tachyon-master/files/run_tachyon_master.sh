#!/bin/bash

$TACHYON_CONTAINER/bin/tachyon bootstrap-conf $MASTER_IP

$TACHYON_CONTAINER/bin/tachyon format

$TACHYON_CONTAINER/bin/tachyon-start.sh master 
