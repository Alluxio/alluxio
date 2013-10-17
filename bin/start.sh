#!/usr/bin/env bash

#start up tachyhon

Usage="Usage: start.sh [-h] WHAT [MOPT]
Where WHAT is one of:
  all MOPT\tStart master and all slaves.
  local\t\tStart a master and slave locally
  master\tStart the master on this node
  safe\t\tScript will run continuously and start the master if it's not
\t\trunning
  worker MOPT\tStart a worker on this node
  workers MOPT\tStart workers on slaves
  restart_worker\tRestart a failed worker on this node
  restart_workers\tRestart any failed workers on slaves

MOPT is one of:
  Mount\t\tMount the configured RamFS
  SudoMount\tMount the configured RamFS using sudo
  NoMount\tDo not mount the configured RamFS

-h  display this help."

bin=`cd "$( dirname "$0" )"; pwd`

ensure_dirs() {
    mkdir -p $TACHYON_HOME/logs
    mkdir -p $TACHYON_HOME/data
}

get_env() {
    if [ -e $TACHYON_HOME/conf/tachyon-env.sh ] ; then
        . $TACHYON_HOME/conf/tachyon-env.sh
    fi
}

check_mount_mode() {
    case "${1}" in
        Mount);;
        SudoMount);;
        NoMount);;
        *)
            if [ -z $1 ]
            then
                echo "This command requires a mount mode be specified"
            else
                echo "Invalid mount mode: $1"
            fi
            echo -e "$Usage"
            exit 1
    esac
}

# pass mode as $2
do_mount() {
    case "${1}" in
        Mount)
            $bin/mount-ramfs.sh $1
            ;;
        SudoMount)
            $bin/mount-ramfs.sh $1
            ;;
        NoMount)
            ;;
        *)
            echo "This command requires a mount mode be specified"
            echo -e "$Usage"
            exit 1
    esac
}

stop() {
    $bin/stop.sh
}


start_master() {
    MASTER_ADDRESS=$TACHYON_MASTER_ADDRESS
    if [ -z $TACHYON_MASTER_ADDRESS ] ; then
        MASTER_ADDRESS=localhost
    fi

    echo "Starting master @ $MASTER_ADDRESS"
    ($JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="MASTER_LOGGER" -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties $TACHYON_JAVA_OPTS tachyon.Master) &
}

start_worker() {
    do_mount $1
    echo "Starting worker @ `hostname`"
    ($JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="WORKER_LOGGER" -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties $TACHYON_JAVA_OPTS tachyon.Worker `hostname` > /dev/null 2>&1 ) &
}

restart_worker() {
    RUN=`ps -ef | grep "tachyon.Worker" | grep "java" | wc | cut -d" " -f7`
    if [[ $RUN -eq 0 ]] ; then
        echo "Restarting worker @ `hostname`"
        ($JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true -Dtachyon.logger.type="WORKER_LOGGER" $TACHYON_JAVA_OPTS tachyon.Worker `hostname`) &
    fi
}

run_on_slaves() {
    HOSTLIST=$bin/../conf/slaves
    for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no $slave $"${@// /\\ }" 2>&1 | sed "s/^/$slave: /" &
        sleep 0.02
    done
}

run_safe() {
    while [ 1 ]
    do
        RUN=`ps -ef | grep "tachyon.Master" | grep "java" | wc | cut -d" " -f7`
        if [[ $RUN -eq 0 ]] ; then
            echo "Restarting the system master..."
            start_master
        fi
        echo "Tachyon is running... "
        sleep 2
    done
}

while getopts "h" o; do
    case "${o}" in
        h)
            echo -e "$Usage"
            exit 0
            ;;
        *)
            echo -e "$Usage"
            exit 1
            ;;
    esac
done

shift $((OPTIND-1))

WHAT=$1

if [ -z "${WHAT}" ]; then
  echo "Error: no WHAT specified"
  echo -e "$Usage"
  exit 1
fi

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

# get env
get_env

# ensure log/data dirs
ensure_dirs

case "${WHAT}" in
    all)
        check_mount_mode $2
        stop $bin
        start_master
        sleep 2
        run_on_slaves $bin/start.sh worker $2
        ;;
    local)
        stop $bin
        sleep 1
        $bin/mount-ramfs.sh SudoMount
        start_master
        sleep 2
        start_worker NoMount
        ;;
    master)
        start_master
        ;;
    worker)
        check_mount_mode $2
        start_worker $2
        ;;
    safe)
        run_safe
        ;;
    workers)
        check_mount_mode $2
        run_on_slaves $bin/start.sh worker $2
        ;;
    restart_worker)
        restart_worker
        ;;
    restart_workers)
        run_on_slaves $bin/start.sh restart_worker
        ;;
    *)
        echo "Error: Invalid WHAT: $WHAT"
        echo -e "$Usage"
        exit 1
esac
