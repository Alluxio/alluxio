# -*- mode: ruby -*-
# vi: set ft=ruby :

def config_vb(config, i, total, name)
  puts "starting " 
  puts Addr[i - 1]
  config.vm.network "private_network", ip: Addr[i - 1]
  config.vm.host_name =  "#{name}"
  if i == total # last VM starts tachyon
    config.vm.provision "shell", path: Post
    config.vm.provision "shell", path: "start_tachyon_cluster.sh"
  end
end
