# -*- mode: ruby -*-
# vi: set ft=ruby :

# AWS specific configurations go here

def config_aws(config, i, total, name)
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
    config.vm.provision "shell", path: "start_tachyon_cluster.sh"
  end

  end
end
