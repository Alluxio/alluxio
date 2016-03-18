# -*- mode: ruby -*-
# vi: set ft=ruby :

# Google specific configurations go here

def config_google(config, i, total, name)
  config.vm.box = "dummy"
  config.vm.box_url = 
    "https://github.com/mitchellh/vagrant-google/raw/master/google.box"
  config.ssh.username = SSH_USERNAME
  config.ssh.private_key_path = KEY_PATH

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider :google do |google, override|
    google.google_project_id = GOOGLE_CLOUD_PROJECT_ID
    google.google_client_email = SERVICE_ACCOUNT_EMAIL_ADDRESS
    google.google_json_key_location = JSON_KEY_LOCATION
  end
end
