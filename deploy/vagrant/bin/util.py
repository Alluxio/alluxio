#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import os
from sys import stderr
import errno


def mkdir_p(path):
    """mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as e:
        if not e.errno == errno.EEXIST:
            raise


def _colorize(code):
    def _(text, bold=False):
        c = code
        if bold:
            c = '1;%s' % c
        return '\033[%sm%s\033[0m' % (c, text)
    return _

_red = _colorize('31')
_green = _colorize('32')
_yellow = _colorize('33')


def info(msg):
    print(_green(">>> " + msg))


def error(msg):
    print(_red(">>> " + msg, True), file=stderr)


def warn(msg):
    print(_yellow(">>> " + msg, True))


