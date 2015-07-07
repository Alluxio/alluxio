#!/bin/bash

IP=$(ip -o -4 addr list eth0 | perl -n -e 'if (m{inet\s([\d\.]+)\/\d+\s}xms) { print $1 }')
echo "NAMESERVER_IP=$IP"

sed -i s/__LOCAL_IP__/$IP/ /etc/dnsmasq.conf

dnsmasq

while [ 1 ];
do
    sleep 3
    # kill and restart dnsmasq every three seconds
    # in case its configuration has changed
    pkill dnsmasq
    dnsmasq
done
