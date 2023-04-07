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
1. Auto configure external python libraries like boto
2. Check environment variables
3. Auto set up security group
"""

import os
import sys
from sys import stderr

from boto import ec2
import yaml

from util import info, warn, error


def get_or_make_group(conn, name, vpc=None):
    groups = [g for g in conn.get_all_security_groups() if (g.name == name)]
    if (vpc == ''):
        vpc = None
    if (vpc is not None):
        groups = [g for g in groups if (g.vpc_id == vpc)]
    if len(groups) > 0:
        return groups[0]
    else:
        info("Creating security group {name} in {region}".format(name=name, region=conn.region))
        group = conn.create_security_group(name, "Auto created by Alluxio deploy", vpc)
        info("Created security group ID {id}".format(id=group.id))
        return group


def set_security_group(conn, name, vpc=None):
    info("Setting up security group {} in {}".format(name, conn.region))
    sg = get_or_make_group(conn, name, vpc)
    if sg.rules != []:
        warn('security group {} in {} already has rules, no modification will happen then'.format(name, conn.region))
        return sg.id
    proto = ['tcp', 'udp']
    authorized_ip = '0.0.0.0/0' # all IP
    for p in proto:
        sg.authorize(p, 0, 65535, authorized_ip)
    return sg.id


def get_aws_secret():
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    if access_key is None:
        error("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set")
        sys.exit(1)

    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if secret_key is None:
        error("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set")
        sys.exit(1)

    return access_key, secret_key


def gen_boto_config(access_key, secret_key):
    home=os.path.expanduser('~')
    boto_config_path = os.path.join(home, '.boto')
    with open(boto_config_path, 'w') as boto_config:
        boto_config.write('\n'.join([
            '[Credentials]',
            'aws_access_key_id = ' + access_key,
            'aws_secret_access_key = ' + secret_key]))


def get_ec2_conf():
    return yaml.load(open('conf/ec2.yml'), Loader=yaml.FullLoader)


def get_conn():
    try:
        conn = ec2.connect_to_region(get_ec2_conf()['Region'])
        return conn
    except Exception as e:
        error(e.message)
        sys.exit(1)


def configure_aws():
    access_key, secret_key = get_aws_secret()
    gen_boto_config(access_key, secret_key)

    conn = get_conn()
    ec2conf = get_ec2_conf()
    return set_security_group(conn, ec2conf['Security_Group'], ec2conf.get('VPC', None))


if __name__ == '__main__':
    print configure_aws()
