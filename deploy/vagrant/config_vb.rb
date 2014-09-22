# -*- mode: ruby -*-
# vi: set ft=ruby :

def config_vb(config, i)
  addr = @cmd['Addresses']
  puts "starting " 
  puts addr[i - 1]
  #v.customize ["modifyvm", :id, "--memory", "3024"]
  config.vm.network "private_network", ip: addr[i - 1]
  config.vm.host_name =  "tachyon#{i}"
end
