# -*- mode: ruby -*-
# vi: set ft=ruby :

def config_docker(d)
  d.build_dir = "../docker/apache-hadoop-hdfs1.0.4-precise"
  d.has_ssh = true 
  d.remains_running = false
end
