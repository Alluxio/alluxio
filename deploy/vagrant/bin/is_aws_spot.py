#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
If 'Spot_Price' is set in 'conf/ec2.yml', print 0, else print 1.
If 'Spot_Price' can not be parsed into 'float', print 1 to stdout and error string to stderr.
"""

from sys import stderr

import yaml

from util import error

def is_spot(price):
    return price is not None and price != ''

if __name__ == '__main__':
    price = yaml.load(open('conf/ec2.yml')).get('Spot_Price')
    if is_spot(price):
      try:
        float(price)
        print(0)
        exit(0)
      except ValueError:
        error("Spot_Price in ec2.yml can be not parsed into float")
    print(1)
