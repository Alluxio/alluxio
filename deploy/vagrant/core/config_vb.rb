# -*- mode: ruby -*-
# vi: set ft=ruby :

# VB specific configurations go here

def config_vb(config, i, total, name)
  puts "starting " + Addr[i - 1]
  config.vm.synced_folder "../../", "/tachyon"
  config.vm.synced_folder "./", "/vagrant"
  config.vm.box = "chef/centos-6.5"
  config.vm.provider "virtualbox" do |vb|
    if Memory != ''
      vb.customize ["modifyvm", :id, "--memory", Memory]
    end
    vb.gui = true
  end

  config.vm.network "private_network", ip: Addr[i - 1]
  config.vm.host_name =  "#{name}"
  if i == total # last VM starts tachyon
    config.vm.provision "shell", path: Post
    config.vm.provision "shell", path: "core/start_tachyon_cluster.sh"
  end
end
