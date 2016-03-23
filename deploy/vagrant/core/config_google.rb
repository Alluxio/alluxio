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
    google.google_client_email = SERVICE_ACCOUNT_EMAIL_ADDRESS
    google.google_json_key_location = JSON_KEY_LOCATION

    google.image = IMAGE
    google.machine_type = MACHINE_TYPE
    google.scopes = SCOPES
    google.disk_size = DISK_SIZE
    google.network = NETWORK
    if (PREFIX != nil and SUBNET != "")
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
