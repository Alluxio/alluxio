#!/usr/bin/env bash

set -e

function main {
    # start mysql
    /usr/bin/mysqld_safe &

    # start hive metastore
    exec hive --service metastore --hiveconf hive.root.logger=INFO,console --hiveconf hive.log.threshold=INFO
}

main "$@"
