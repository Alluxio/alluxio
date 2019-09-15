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


Design:


https://docs.google.com/document/d/1pQDKQnivw9-xthV1BoNsw4gnXuzhxMZ2BVfkh56FIGY/edit#