#!/bin/bash

TACHYON_MOUNT=/opt/tachyon_host

function start_master() {
  echo "starting master container"
  local hostname=master
  echo "local tachyon directory is resolved to: $TACHYON_DIR"
  MASTER=$(sudo docker run --privileged="true" -d --dns $NAMESERVER_IP -h $hostname -v $TACHYON_DIR:$TACHYON_MOUNT "$1:$2")
  if [ "$MASTER" == "" ]; then
    echo "error: could not start master container from image $1"
    exit 1
  fi
  echo "started master container: $MASTER"
  sleep 3
  MASTER_IP=$(sudo docker logs $MASTER 2>&1 | egrep '^MASTER_IP=' | awk -F= '{print $2}' | tr -d -c "[:digit:] .")
  echo "MASTER_IP:                $MASTER_IP"
  echo "address=\"/$hostname/$MASTER_IP\"" >> $DNSFILE
}

function start_worker() {
  local hostname="$3"
  echo "starting $hostname"
  WORKER=$(sudo docker run -d --dns $NAMESERVER_IP -h $hostname -v $TACHYON_DIR:$TACHYON_MOUNT "$1:$2" $MASTER_IP)
  if [ "$WORKER" == "" ]; then
    echo "error: could not start worker container from image $1"
  fi
  echo "started worker container: $WORKER"
  sleep 5
  WORKER_IP=$(sudo docker logs $WORKER 2>&1 | egrep '^WORKER_IP=' | awk -F= '{print $2}' | tr -d -c "[:digit:] .")
  echo "WORKER_IP:             $WORKER_IP"
  echo "address=\"/$hostname/$WORKER_IP\"" >> $DNSFILE
}

function start_workers() {
  for i in `seq 1 $NUM_WORKERS`; do
    echo "NO.$i remote worker"
    start_worker $1 $2 "worker$i"
  done
}

function print_cluster_info() {
  echo "**************************************************"
  echo ""
  echo "visit Tachyon Web UI at: http://$MASTER_IP:19999"
  echo ""
  echo "ssh into master via:     ssh -i $SSH_ID_RSA_DIR -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@${MASTER_IP}"
  echo "you can also ssh into workers via the command above with the ip substituted"
  echo ""
  echo "after ssh into either master/worker, /root/tachyon_container is tachyon home"
  echo ""
  echo "to enable the host to resolve {'master', 'worker1', 'worker2'...} to corresponding ip, set 'nameserver $NAMESERVER_IP' as first line in your host's /etc/resolv.conf"
  echo ""
  echo "**************************************************"
}
