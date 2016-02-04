# -*- mode: ruby -*-
# vi: set ft=ruby :

# VB specific configurations go here

require 'yaml'

def config_vb(config, i, total, name, tachyon_is_local)
  # sync vagrant/shared, but shared may be created in vm, so we sync vagrant/
  # we can put maven repos, hadoop binary tar to vagrant/shared so that they
  # only need to be downloaded once, this is valuable for development on laptop :)
  # for other providers, since the deployment is on "cloud", this feature is disabled so
  # each vm will download in parallel
  config.vm.synced_folder ".", "/vagrant"

  if tachyon_is_local
    config.vm.synced_folder "../../", "/alluxio"
  end

  config.vm.box = "alluxio-dev"
  config.vm.provider "virtualbox" do |vb|
    mem = YAML.load_file('conf/vb.yml')['MachineMemory']
    if mem != ''
      vb.customize ["modifyvm", :id, "--memory", mem]
    end
    vb.gui = true
  end

  config.vm.network "private_network", type: "dhcp"

  config.ssh.insert_key = false
end
