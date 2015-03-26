---
layout: global
title: Deploy Specific Tachyon Commit Via Vagrant
---

In deploy/vagrant/tachyon_version.yml:

```yaml
# {Github | Local}
Type: Github

# github repo and commit hash 
Github: 
  Repo: https://github.com/amplab/tachyon
  Hash: eaeaccd19832494c5f0b2a792888a5eaacd7bcf8
```

You can set `Type` to `Local` to deploy the Tachyon built locally in your Tachyon project's root directory

Set `Type` to `Github`, and specify `Repo` and `Hash` to clone the repo to deploy/tachyon, and checkout the specified commit hash as a new branch, then deploy this specific commit

After configuration, follow [Running-Tachyon-on-VirtualBox](Running-Tachyon-on-VirtualBox.html) or
[Running-Tachyon-on-Container](Running-Tachyon-on-Container.html) or [Running-Tachyon-on-AWS](Running-Tachyon-on-AWS.html)
or [Running-Tachyon-on-OpenStack](Running-Tachyon-on-OpenStack.html)
