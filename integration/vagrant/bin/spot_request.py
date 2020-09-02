#!/usr/bin/env python
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

# -*- coding: utf-8 -*-

"""
Submit or Cancel spot instance requests.

When submit, the process will block until the request is fulfilled
or the process is killed by user(like CTRL + C),
if the process is killed, the requests will be automatically canceled.
"""

import os
import time
import pickle
import argparse
import subprocess

import yaml
import boto.ec2.blockdevicemapping as bdm

from util import mkdir_p, info, warn, error
from init_aws import get_conn, get_ec2_conf


def get_bdm(ec2_conf):
    def device(d):
        dev = bdm.BlockDeviceType()
        if d['VirtualName'].startswith('ephemeral'):
            # Instance Storage
            dev.ephemeral_name = d['VirtualName']
        else:
            # EBS
            dev.size = d['Ebs.VolumeSize']
            delete = d.get('Ebs.DeleteOnTermination', None)
            if delete is not None:
                dev.delete_on_termination = delete
        return (d['DeviceName'], dev)
    devices = map(device, ec2_conf['Block_Device_Mapping'])
    device_mapping = bdm.BlockDeviceMapping()
    for name, dev in devices:
        device_mapping[name] = dev
    return device_mapping


def get_init_conf():
    return yaml.load(open('conf/init.yml'))


class RequestFailedError(Exception): pass


def all_fulfilled(requests):
    fulfilled = True
    for r in requests:
        if r.status.code != 'fulfilled':
            fulfilled = False
            if r.state == 'failed':
                raise RequestFailedError(r.status.message)
        if not fulfilled:
            break
    return fulfilled


def wait_until_fulfilled(request_ids, conn):
    while True:
        requests = conn.get_all_spot_instance_requests(request_ids)
        if not all_fulfilled(requests):
            time.sleep(1)
        else:
            return requests


def add_tag(host):
    return '{}-{}'.format(get_ec2_conf()['Tag'], host)


def get_host(tag):
    return tag.split('-')[-1]


# request_id -> tag
def request_id_to_tag(requests, masters):
    ret = {}
    for i, rid in enumerate([r.id for r in requests]):
        # TODO(cc): This naming convention for host may need changes
        if i == 0:
            host = 'AlluxioMaster'
        elif i < masters:
            host = 'AlluxioMaster{}'.format(i + 1)
        else:
            host = 'AlluxioWorker{}'.format(i - masters + 1)
        ret[rid] = add_tag(host)
    return ret


def save_request_ids(request_ids):
    out = open('.request_ids', 'w')
    pickle.dump(request_ids, out)
    out.close()


def load_request_ids():
    return pickle.load(open('.request_ids'))


def submit_request(conn, ec2_conf, masters):
    # enable ssh as root without tty
    user_data = "#!/bin/bash\n \
        echo 'Defaults:root !requiretty' > /etc/sudoers.d/998-vagrant-cloud-init-requiretty && \
        echo 'Defaults:ec2-user !requiretty' > /etc/sudoers.d/999-vagrant-cloud-init-requiretty && \
        chmod 440 /etc/sudoers.d/998-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/999-vagrant-cloud-init-requiretty"

    requests = conn.request_spot_instances(
        price = ec2_conf['Spot_Price'],
        image_id = ec2_conf['AMI'],
        count = get_init_conf()['MachineNumber'],
        availability_zone_group = ec2_conf['Availability_Zone'],
        placement = ec2_conf['Availability_Zone'], # where to put instance
        key_name = ec2_conf['Keypair'],
        security_groups = [ec2_conf['Security_Group']],
        user_data = user_data,
        instance_type = ec2_conf['Instance_Type'],
        block_device_map = get_bdm(ec2_conf))
    request_ids = [r.id for r in requests]
    save_request_ids(request_ids)

    # sleep before waiting for spot instances to be fulfilled.
    time.sleep(5)

    # block, waiting for all requests to be fulfilled
    requests = wait_until_fulfilled(request_ids, conn)

    # tag the requests and instances
    rid_tag = request_id_to_tag(requests, masters)
    for r in requests:
        tag = rid_tag[r.id]
        r.add_tag('Name', tag)
        conn.create_tags([r.instance_id], {'Name': tag})

    return rid_tag, requests


def cancel_request(conn):
    warn('canceling spot instance requests and terminating instances...')
    requests = conn.get_all_spot_instance_requests(load_request_ids())
    for r in requests:
        r.cancel()
    instance_ids = [r.instance_id for r in requests if r.instance_id is not None]
    if len(instance_ids) > 0:
        conn.terminate_instances(instance_ids)


# mock the inventory file and machine id files that should have
# been generated by vagrant, so that we can keep the vagrant work flow.
def mock_vagrant_info(instance_id_to_tag_ip):
    inventory_dir = '.vagrant/provisioners/ansible/inventory'
    mkdir_p(inventory_dir)
    inventory = open(os.path.join(inventory_dir, 'vagrant_ansible_inventory'), 'w')
    for instance_id, tag_ip in instance_id_to_tag_ip.iteritems():
        tag, ip = tag_ip
        host = get_host(tag)

        inventory.write("{} ansible_ssh_host={} ansible_ssh_port=22\n".format(host, ip))

        id_dir = os.path.join('.vagrant', 'machines', host, 'aws')
        mkdir_p(id_dir)
        with open(os.path.join(id_dir, 'id'), 'w') as f:
            f.write(instance_id)
    inventory.close()


def is_ssh_ready(host):
    s = subprocess.Popen(['ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'ConnectTimeout=30',
        '-i', os.path.expanduser(get_ec2_conf()['Key_Path']),
        '%s@%s' % ('ec2-user', host),
        'true'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    s.communicate()
    return s.returncode == 0


def wait_for_ssh(hosts):
    while len(hosts):
        hosts = [h for h in hosts if not is_ssh_ready(h)]


def parse():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('-s', '--submit', action='store_true')
    grp.add_argument('-c', '--cancel', action='store_true')
    parser.add_argument('--masters', type=int, default=1, help='number of Alluxio masters')
    return parser.parse_args()


def main(args):
    ec2_conf = get_ec2_conf()
    conn = get_conn()
    if args.submit:
        info('waiting for spot instance requests to be fulfilled, you can cancel by ctrl+c ...')
        try:
            rid_tag, requests = submit_request(conn, ec2_conf, args.masters)
        except (KeyboardInterrupt, RequestFailedError) as e:
            error(e)
            exit(1)
        info('spot instance requests fulfilled')
        instance_id_to_tag_ip = {}
        info('getting instance IPs...')
        for r in requests:
            instance_id = r.instance_id
            info('waiting for ip to be allocated to the machine')
            ip = conn.get_only_instances([instance_id])[0].ip_address
            while ip is None:
                time.sleep(1)
                ip = conn.get_only_instances([instance_id])[0].ip_address
            instance_id_to_tag_ip[instance_id] = (rid_tag[r.id], ip)
        info('mocking vagrant info under .vagrant...')
        mock_vagrant_info(instance_id_to_tag_ip)
        info('creation of spot instances done')
        info('waiting for ssh to be available...')
        wait_for_ssh([ip for tag, ip in instance_id_to_tag_ip.values()])
        info('ssh for all instances are ready')
    elif args.cancel:
        cancel_request(conn)

if __name__ == '__main__':
    main(parse())
