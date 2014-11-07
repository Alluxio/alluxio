# -*- mode: ruby -*-
# vi: set ft=ruby :

# OpenStack specific configurations go here

def config_os(config, i, total, name)        
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
    config.vm.provision "shell", inline: 'sudo sed -i -e "s/PasswordAuthentication no/PasswordAuthentication yes/g" /etc/ssh/sshd_config'
    if i == total # last VM starts tachyon
      config.vm.provision "shell", path: Post
      config.vm.provision "shell", path: "start_tachyon_cluster.sh"
    end
  end
end
