#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sys import stderr

import yaml

from util import error

price = yaml.load(open('conf/ec2.yml')).get('Spot_Price')
use_spot = price is not None and price != ''
if use_spot:
  try:
    float(price)
    print(0)
    exit(0)
  except Exception:
    error("Spot_Price in ec2.yml can be not parsed into float")
print(1)
