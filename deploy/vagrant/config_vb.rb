# -*- mode: ruby -*-
# vi: set ft=ruby :

def config_vb(v, i)
  addr = @cmd['addresses']
  puts "starting " 
  puts addr[i - 1]
  v.vm.network "private_network", ip: addr[i - 1]
  v.vm.host_name =  "tachyon#{i}"
end
