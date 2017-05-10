#!/bin/bash

$TACHYON_CONTAINER/bin/tachyon bootstrap-conf $MASTER_IP

$TACHYON_CONTAINER/bin/tachyon-start.sh worker SudoMount
