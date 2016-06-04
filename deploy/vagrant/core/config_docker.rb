# -*- mode: ruby -*-
# vi: set ft=ruby :

# Docker specific configurations go here

def config_docker(config, i, total, name)
  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.ssh.username = "root"
  config.ssh.password = "vagrant"
  config.ssh.private_key_path = "files/id_rsa"
  config.vm.provider "docker" do |d|
    d.build_dir = "."
    config.ssh.port ="22"
    d.has_ssh = true 
    d.create_args = ["--privileged"]
    d.remains_running = true
  end
end
