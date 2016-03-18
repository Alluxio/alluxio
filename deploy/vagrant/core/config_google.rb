# -*- mode: ruby -*-
# vi: set ft=ruby :

# Google specific configurations go here

def config_google(config, i, total, name)
  config.vm.box = "dummy"
  config.vm.box_url = 
    "https://github.com/mitchellh/vagrant-google/raw/master/google.box"
  config.ssh.username = "ericand"
  config.ssh.private_key_path = "~/.ssh/google_compute_engine" #KEY_PATH

  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider :google do |google, override|
    #google.google_project_id = "ericand-sandbox"
    #google.google_client_email = "eric-laptop@ericand-sandbox.iam.gserviceaccount.com"
    #google.google_json_key_location = "/users/ericand/keys/ericand-sandbox-d90e056d395a.json"
    google.google_project_id = GOOGLE_CLOUD_PROJECT_ID
    google.google_client_email = SERVICE_ACCOUNT_EMAIL_ADDRESS
    google.google_json_key_location = JSON_KEY_LOCATION
    #google.tags = {
    #  'Name' => TAG + "-" + name,
    #}
  end
end
