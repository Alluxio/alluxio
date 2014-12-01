# -*- mode: ruby -*-
# vi: set ft=ruby :

# Docker specific configurations go here

def config_docker(config, i, total, name)
  config.vm.synced_folder "../../", "/tachyon"
  config.vm.synced_folder "./", "/vagrant"
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

  config.vm.host_name =  "#{name}"
  if i == total # last VM starts tachyon
    config.vm.provision "shell", path: Post
    config.vm.provision "shell", path: "core/start_tachyon_cluster.sh"
  end
end
