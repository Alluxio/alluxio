# Ufs = {glusterfs|ceph|hadoop2|hadoop1}
Ufs: 'hadoop2'
# Provider = {vb|docker|aws|openstack}
Provider: 'openstack'
# Memory is the amount memory allocated to each VM
Memory: 1024 #MB
# Total is the number of VMs to start
Total: 2
# Addresses are the internal IPs given to each VM. 
# The last one is designated as Tachyon master
# For VirtualBox: the addresses can be arbitrary
# For AWS: the addresses should be within the same availability zone
# For OpenStack: these addresses are not used. 
Addresses:
    - "n/a"
    - "n/a"
