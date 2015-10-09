---
layout: global
title: Configuring Tachyon with GlusterFS
nickname: Tachyon with GlusterFS
group: Under Stores
priority: 4
---

This guide describes how to configure Tachyon with [Swift](http://www.gluster.org/) as the under storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either [compile Tachyon](Building-Tachyon-Master-Branch.html), or [download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

# Configuring Tachyon
