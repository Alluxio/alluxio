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

# Google specific configurations go here

def config_google(config, i, total, name)
  config.vm.box = "google"
  config.vm.box_url = 
    "https://github.com/mitchellh/vagrant-google/raw/master/google.box"
  config.ssh.username = SSH_USERNAME
  config.ssh.private_key_path = KEY_PATH

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider :google do |google, override|
    google.google_project_id = GOOGLE_CLOUD_PROJECT_ID
    google.google_json_key_location = JSON_KEY_LOCATION

    google.image_family = IMAGE_FAMILY
    google.image_project_id = IMAGE_PROJECT_ID
    google.machine_type = MACHINE_TYPE
    google.scopes = SCOPES
    google.disk_size = DISK_SIZE
    google.network = NETWORK
    if (INSTANCE_GROUP != nil)
      google.instance_group = INSTANCE_GROUP
    end
    if (PREFIX != nil)
      google.name = PREFIX + "-" + name.downcase
    else
      google.name = name.downcase
    end
    if PREEMPTIBLE == true
      google.preemptible = true
      google.auto_restart = false
      google.on_host_maintenance = "TERMINATE"
    end
    google.metadata = {"startup-script" => "#!/bin/bash\necho 'Defaults:root !requiretty' > /etc/sudoers.d/gce && echo 'Defaults:#{SSH_USERNAME} !requiretty' >> /etc/sudoers.d/gce"}
  end
end
