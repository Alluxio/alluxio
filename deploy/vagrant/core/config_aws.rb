# -*- mode: ruby -*-
# vi: set ft=ruby :

# AWS specific configurations go here

def config_aws(config, i, total, name)
  config.vm.box = "dummy"
  config.vm.box_url = 
    "https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box"
  config.ssh.username = "ec2-user"
  config.ssh.private_key_path = KEY_PATH

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider :aws do |aws, override|
    aws.keypair_name = KEYPAIR
    aws.security_groups = SECURITY_GROUP
    aws.ami = AMI
    aws.region = REGION
    aws.instance_type = INSTANCE_TYPE
    aws.block_device_mapping = BLOCK_DEVICE_MAPPING
    aws.tags = {
      'Name' => TAG + name,
    }
	  aws.availability_zone = AVAILABILITY_ZONE
    aws.user_data = "#!/bin/bash\necho 'Defaults:root !requiretty' > /etc/sudoers.d/998-vagrant-cloud-init-requiretty && echo 'Defaults:ec2-user !requiretty' > /etc/sudoers.d/999-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/998-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/999-vagrant-cloud-init-requiretty"
  end
end
