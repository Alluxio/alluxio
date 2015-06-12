#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
1. Auto configure external python libraries like boto
2. Check environment variables 
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
