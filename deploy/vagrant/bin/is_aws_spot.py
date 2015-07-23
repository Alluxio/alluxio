#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Check whether to use AWS spot instance

If true, create vars for ansible playbook in spot/
"""

import os
import tempfile

import yaml

from util import mkdir_p


def is_aws_spot(ec2_conf):
    if ec2_conf['Spot_Price'] == "":
        return False
    else:
        return True


def trans_to_volume(device):
    volume = {}
    volume['device_name'] = device['DeviceName']
    if device['VirtualName'].startswith('ephemeral'):
        # instance storage
        volume['ephemeral'] = device['VirtualName']
    else:
        # EBS
        volume['volume_size'] = device['Ebs.VolumeSize']
        delete = device.get('Ebs.DeleteOnTermination', None)
        if delete is not None:
            volume['delete_on_termination'] = delete
    return volume


def create_aws_spot_vars(ec2_conf):
    init = yaml.load(open("conf/init.yml"))
    volumes = map(trans_to_volume, ec2_conf["Block_Device_Mapping"])
    # temporary folder for ansible to save info
    ansible_info_dir = tempfile.mkdtemp()

    var = {
        'zone':           ec2_conf["Availability_Zone"],
        'count':          init["MachineNumber"],
        'image':          ec2_conf["AMI"],
        'region':         ec2_conf["Region"],
        'volumes':        volumes,
        'key_name':       ec2_conf["Keypair"],
        'spot_price':     ec2_conf["Spot_Price"],
        'instance_type':  ec2_conf["Instance_Type"],
        'security_group': ec2_conf["Security_Group"],
        'ansible_info_dir': ansible_info_dir,
    }

    var_dir = 'spot/roles/create_ec2/vars'
    mkdir_p(var_dir)
    out = open(os.path.join(var_dir, 'main.yml'), 'w')
    yaml.dump(var, out, default_flow_style=False)
    out.close()


if __name__ == '__main__':
    ec2_conf = yaml.load(open('conf/ec2.yml'))
    is_spot = is_aws_spot(ec2_conf)
    if is_spot:
        print('true')
        create_aws_spot_vars(ec2_conf)
    else:
        print('false')
