#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

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
    aws.access_key_id = ENV['AWS_ACCESS_KEY_ID']
    aws.secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    aws.keypair_name = KEYPAIR
    aws.ami = AMI
    aws.region = REGION
    aws.instance_type = INSTANCE_TYPE
    aws.block_device_mapping = BLOCK_DEVICE_MAPPING
    aws.tags = {
      'Name' => TAG + "-" + name,
    }
	  aws.availability_zone = AVAILABILITY_ZONE
    if (SUBNET != nil and SUBNET != "")
      aws.subnet_id = SUBNET
      aws.associate_public_ip = TRUE
      aws.security_groups = ENV['AWS_SECURITY_GROUP_ID_TACH']
    else
      aws.security_groups = SECURITY_GROUP
    end
    aws.user_data = "#!/bin/bash\necho 'Defaults:root !requiretty' > /etc/sudoers.d/998-vagrant-cloud-init-requiretty && echo 'Defaults:ec2-user !requiretty' > /etc/sudoers.d/999-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/998-vagrant-cloud-init-requiretty && chmod 440 /etc/sudoers.d/999-vagrant-cloud-init-requiretty"
  end
end
