#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import errno


def mkdir_p(path):
    """mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as e:
        if not e.errno == errno.EEXIST:
            raise
