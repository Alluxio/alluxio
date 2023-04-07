0.1.0

- Init support
- Modularize the directory structure
- Java and docker natively integration(Consider memory, https://developers.redhat.com/blog/2017/03/14/java-inside-docker/)
- Make more configurable, like node selector, tolerance


0.2.0

- Two choices to decide short circuit: copying from directory directly or unix socket
- simplify tiered storage (Combine alluxio properties and Persistent volume)
- Use init container to do format /no format with intelligence
- Fuse daemonset



0.3.0

- Both support one layer contains multiple storages(https://docs.alluxio.io/os/user/stable/en/advanced/Alluxio-Storage-Management.html#single-tier-storage) and multiple layers


0.4.0

- Add host pid for master

0.5.0

- Add envs for fuse


0.6.0

- change envs to env


0.7.0

- make hostNetwork, hostPID configurable


0.8.0

- Use local timezone

0.9.0

- Support helm v3

0.10.0

- Fix alluxio-config

0.11.0

- Get memory capcity from node label

0.12.0

- Change from metadata.labels['data.alluxio.io/storage-GB-alluxio-{{ .Release.Name }}'] to metadata.labels['data.alluxio.io/storage-human-alluxio-{{ .Release.Name }}']

Design:


https://docs.google.com/document/d/1pQDKQnivw9-xthV1BoNsw4gnXuzhxMZ2BVfkh56FIGY/edit#
