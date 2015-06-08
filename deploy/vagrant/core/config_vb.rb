# -*- mode: ruby -*-
# vi: set ft=ruby :

# VB specific configurations go here

def config_vb(config, i, total, name, tachyon_is_local)
  puts "starting " + Addr[i - 1]

  # sync vagrant/shared, but shared may be created in vm, so we sync vagrant/
  # we can put maven repos, hadoop binary tar to vagrant/shared so that they
  # only need to be downloaded once, this is valuable for development on laptop :)
  # for other providers, since the deployment is on "cloud", this feature is disabled so
  # each vm will download in parallel
  config.vm.synced_folder ".", "/vagrant"

  if tachyon_is_local
    config.vm.synced_folder "../../", "/tachyon"
  end

  config.vm.box = "tachyon-dev"
  config.vm.provider "virtualbox" do |vb|
    if Memory != ''
      vb.customize ["modifyvm", :id, "--memory", Memory]
    end
    vb.gui = true
  end

  config.vm.network "private_network", ip: Addr[i - 1]

  config.ssh.insert_key = false
end
