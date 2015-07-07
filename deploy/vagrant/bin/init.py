#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
1. Auto configure external python libraries like boto
2. Check environment variables 
3. Auto set up security group
"""

from __future__ import print_function, with_statement

import os
import sys
from sys import stderr
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('provider')
    args = parser.parse_args()
    return args


def get_or_make_group(conn, name):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group {name} in {region}".format(name=name, region=conn.region))
        return conn.create_security_group(name, "Auto created by Tachyon deploy")


def set_security_group(conn, name):
    print("Setting up security group {} in {}".format(name, conn.region))
    sg = get_or_make_group(conn, name)
    if sg.rules != []:
        print('security group {} in {} already has rules, no modification will happen then'.format(name, conn.region))
        return
    proto = ['tcp', 'udp']
    authorized_ip = '0.0.0.0/0' # all IP
    for p in proto:
        sg.authorize(p, 0, 65535, authorized_ip)
 

args = parse_args()
if args.provider == 'aws':
    access_key = os.getenv('AWS_ACCESS_KEY_ID') 
    if access_key is None:
        print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set", 
                file=stderr)
        sys.exit(1)

    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') 
    if secret_key is None:
        print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set", 
                file=stderr)
        sys.exit(1)

    home=os.path.expanduser('~')
    boto_config_path = os.path.join(home, '.boto')
    with open(boto_config_path, 'w') as boto_config:
        boto_config.write('\n'.join([
            '[Credentials]', 
            'aws_access_key_id = ' + access_key, 
            'aws_secret_access_key = ' + secret_key]))

    import boto
    from boto import ec2
    import yaml

    ec2_conf = yaml.load(open('conf/ec2.yml'))
    region = ec2_conf['Region']
    sg = ec2_conf['Security_Group']

    try:
        conn = ec2.connect_to_region(region)
        print(conn.region)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)
    set_security_group(conn, sg)

