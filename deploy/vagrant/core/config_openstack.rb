# -*- mode: ruby -*-
# vi: set ft=ruby :

# OpenStack specific configurations go here

def config_os(config, i, total, name)
  config.vm.box = "dummy"
  config.vm.box_url = 
    "https://github.com/cloudbau/vagrant-openstack-plugin/raw/master/dummy.box"
  # Make sure the private key from the key pair is provided
  config.ssh.private_key_path = KEY_PATH

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider :openstack do |os|
    os.username     = ENV['OS_USERNAME']
    os.api_key      = ENV['OS_PASSWORD']
    os.flavor       = FLAVOR
    os.image        = /#{Regexp.quote(IMAGE)}/
    os.endpoint     = KEYSTONE
    os.security_groups = SECURITY_GROUP
    os.ssh_username = SSH_USERNAME
    os.keypair_name = KEYPAIR_NAME
    os.floating_ip = "auto"
    os.server_name = TAG + name
  end

end
