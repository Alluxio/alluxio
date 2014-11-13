# -*- mode: ruby -*-
# vi: set ft=ruby :

# AWS specific configurations go here

def config_aws(config, i, total, name)
  config.vm.box = "dummy"
  config.vm.box_url = 
    "https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box"
  config.ssh.username = "ec2-user"
  config.ssh.private_key_path = KEY_PATH
  config.vm.synced_folder "../../", "/tachyon", type: "rsync", 
            rsync__exclude: ["../../.git/", "shared/"]
  config.vm.synced_folder "./", "/vagrant", type: "rsync", 
            rsync__exclude: ["shared/"]

  config.vm.provider :aws do |aws, override|
    aws.keypair_name = KEYPAIR
    aws.security_groups = SECURITY_GROUP
    aws.ami = AMI
    aws.region = REGION
    aws.instance_type = INSTANCE_TYPE
    aws.tags = {
      'Name' => TAG + name,
    }
	aws.availability_zone = AVAILABILITY_ZONE
    aws.private_ip_address = Addr[i - 1]
    aws.user_data = "#!/bin/bash\necho 'Defaults:root !requiretty' > /etc/sudoers.d/998-vagrant-cloud-init-requiretty && echo 'Defaults:ec2-user !requiretty' > /etc/sudoers.d/999-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/998-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/999-vagrant-cloud-init-requiretty"      
  if i == total # last VM starts tachyon
    config.vm.provision "shell", path: Post
    config.vm.provision "shell", path: "core/start_tachyon_cluster.sh"
  end

  end
end
