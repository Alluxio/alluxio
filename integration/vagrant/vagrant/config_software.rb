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

require 'yaml'

class Zookeeper
  def initialize(yaml_path)
    puts 'Parsing zookeeper.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @dist = ''
    case @type
    when "Release"
      @version = @yml['Release']['Version']
      puts "Using Zookeeper version #{@version}"
    when "None"
      puts 'No Zookeeper will be set up'
    else
      puts "ERROR: Unknown Zookeeper type #{@type}"
      exit(1)
    end
  end

  def type
    return @type
  end

  def version
    return @version
  end
end

class Alluxio
  def initialize(yaml_path)
    puts 'Parsing alluxio.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @repo = ''
    @version = ''
    case @type
    when "Local"
      puts 'Using local Alluxio repository'
    when "Github"
      @repo = @yml['Github']['Repo']
      @version = @yml['Github']['Version']
      if @version.start_with?("branch-")
        major, minor = @version.sub("branch-", "").split(".")
      end
      puts "Using github #{@repo}, version #{@version}"
    when "Release"
      @version = @yml['Release']['Version']
      major, minor = @version.split(".")
      major = Integer(major) rescue nil
      minor = Integer(minor) rescue nil
      if (major < 1) or (major == 1 and minor < 4)
        puts "ERROR: Only support Alluxio version 1.4.0 and later, check downloads.alluxio.io."
        exit(1)
      end
      puts "Using Alluxio version #{@version}"
    else
      puts "ERROR: Unknown Alluxio type #{@type}"
      exit(1)
    end
    @mem = @yml['WorkerMemory']
    @masters = @yml['Masters']
  end

  def type
    return @type
  end

  def repo_version
    return @repo, @version
  end

  def memory
    return @mem
  end

  def masters
    return @masters
  end
end

class Mesos
  def initialize(yaml_path)
    puts 'Parsing mesos.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @repo = ''
    @version = ''
    @dist = ''
    @use_mesos = true
    case @type
    when "Github"
      @repo = @yml['Github']['Repo']
      @version = @yml['Github']['Version']
      puts "Using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "Using Mesos distribution #{@dist}"
    when "None"
      puts 'No Mesos will be set up'
      @use_mesos = false
    else
      puts "ERROR: Unknown Mesos type #{@type}"
      exit(1)
    end
  end

  def dist
    return @dist
  end

  def type
    return @type
  end

  def use_mesos
    return @use_mesos
  end

  def repo_version
    return @repo, @version
  end
end

class Spark
  def initialize(yaml_path)
    puts 'Parsing spark.yml'
    @yml = YAML.load_file(yaml_path)

    @use_spark = true
    @repo = ''
    @version = ''
    @dist = ''
    case @yml['Type']
    when "None"
      puts 'No Spark will be set up'
      @use_spark = false
    when "Github"
      @git = @yml['Github']
      @repo = @git['Repo']
      @version = @git['Version']
      puts "Using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "Using Spark distribution #{@dist}"
    else
      puts "ERROR: Unknown Spark type #{@yml['Type']}"
      exit(1)
    end
  end

  def type
    return @yml['Type']
  end

  def dist
    return @dist
  end

  def use_spark
    return @use_spark
  end

  def repo_version
    return @repo, @version
  end

end

class Hadoop
  def initialize(yml)
    @version = ''
    @spark_profile = ''
    if yml == nil
      return
    end
    @version = yml['Version']
    if @version == nil
      puts 'ERROR: Hadoop:Version is not set'
      exit(1)
    end
    @spark_profile = yml['SparkProfile'] or @spark_profile
    @hadoop_tarball_url = "http://archive.apache.org/dist/hadoop/common/hadoop-#{@version}/hadoop-#{@version}.tar.gz"
  end

  def tarball_url
    return @hadoop_tarball_url
  end

  def version
    return @version
  end

  def spark_profile
    return @spark_profile
  end

  def alluxio_dist(alluxio_version)
    if alluxio_version.start_with?("1")
      hadoop_major_minor_version = @version.split('.')[0..1].join('.')
      return "alluxio-#{alluxio_version}-hadoop-#{hadoop_major_minor_version}-bin.tar.gz"
    end
    return "alluxio-#{alluxio_version}-bin.tar.gz"
  end
end

class S3
  def initialize(yml)
    @bucket = ''
    @id = ''
    @key = ''

    if yml == nil
      return
    end

    @bucket = yml['Bucket']
    if @bucket == nil
      puts 'ERROR: S3:Bucket is not set'
      exit(1)
    end
    @id = ENV['AWS_ACCESS_KEY_ID']
    if @id == nil
      puts 'ERROR: AWS_ACCESS_KEY_ID needs to be set as environment variable'
      exit(1)
    end
    @key = ENV['AWS_SECRET_ACCESS_KEY']
    if @key == nil
      puts 'ERROR: AWS_SECRET_ACCESS_KEY needs to be set as environment variable'
      exit(1)
    end
  end

  def id
    return @id
  end

  def key
    return @key
  end

  def bucket
    return @bucket
  end

  def alluxio_dist(alluxio_version)
    # The base version should work for S3
    return "alluxio-#{alluxio_version}-bin.tar.gz"
  end
end

class Swift
  def initialize(yml)
    @container = ''
    @user = ''
    @tenant = ''
    @password = ''
    @auth_url = ''
    @user_public_url = ''
    @auth_method = ''

    if yml == nil
      return
    end

    @container = yml['Container']
    if @container == nil
      puts 'ERROR: Swift:Container is not set'
      exit(1)
    end
    @user = ENV['SWIFT_USER']
    if @user == nil
      puts 'ERROR: SWIFT_USER needs to be set as environment variable'
      exit(1)
    end
    @tenant = ENV['SWIFT_TENANT']
    if @tenant == nil
      puts 'ERROR: SWIFT_TENANT needs to be set as environment variable'
      exit(1)
    end
    @password = ENV['SWIFT_PASSWORD']
    if @password == nil
      puts 'ERROR: SWIFT_PASSWORD needs to be set as environment variable'
      exit(1)
    end
    @auth_url = ENV['SWIFT_AUTH_URL']
    if @auth_url == nil
      puts 'ERROR: SWIFT_AUTH_URL needs to be set as environment variable'
      exit(1)
    end
    @use_public_url = ENV['SWIFT_USE_PUBLIC_URL']
    if @use_public_url == nil
      puts 'ERROR: SWIFT_USE_PUBLIC_URL needs to be set as environment variable'
      exit(1)
    end
    @auth_method = ENV['SWIFT_AUTH_METHOD']
    if @auth_method == nil
      puts 'ERROR: SWIFT_AUTH_METHOD needs to be set as environment variable'
      exit(1)
    end
  end

  def user
    return @user
  end

  def tenant
    return @tenant
  end

  def password
    return @password
  end

  def auth_url
    return @auth_url
  end

  def use_public_url
    return @use_public_url
  end

  def auth_method
    return @auth_method
  end

  def container
    return @container
  end

  def alluxio_dist(alluxio_version)
    # The base version should work for Swift
    return "alluxio-#{alluxio_version}-bin.tar.gz"
  end
end

class GCS
  def initialize(yml)
    @bucket = ''
    @id = ''
    @key = ''

    if yml == nil
      return
    end

    @bucket = yml['Bucket']
    if @bucket == nil
      puts 'ERROR: GCS:Bucket is not set'
      exit(1)
    end
    @id = ENV['GCS_ACCESS_KEY_ID']
    if @id == nil
      puts 'ERROR: GCS_ACCESS_KEY_ID needs to be set as environment variable'
      exit(1)
    end
    @key = ENV['GCS_SECRET_ACCESS_KEY']
    if @key == nil
      puts 'ERROR: GCS_SECRET_ACCESS_KEY needs to be set as environment variable'
      exit(1)
    end
  end

  def id
    return @id
  end

  def key
    return @key
  end

  def bucket
    return @bucket
  end

  def alluxio_dist(alluxio_version)
    # The base version should work for GCS
    return "alluxio-#{alluxio_version}-bin.tar.gz"
  end
end

class Ufs
  def get_default_ufs(provider)
    case provider
    when 'vb'
      puts 'Using hadoop2 as default ufs'
      return 'hadoop2'
    when 'google'
      puts 'Using gcs as default ufs'
      return 'gcs'
    when 'aws'
      puts 'Using s3 as default ufs'
      return 's3'
    else
      return ''
    end
  end

  def initialize(yaml_path, provider)
    @yml = YAML.load_file(yaml_path)
    if @yml['Type'] == nil
      @yml['Type'] = get_default_ufs(provider)
    end
    puts @yml

    @hadoop = Hadoop.new(nil)
    @s3 = S3.new(nil)
    @gcs = GCS.new(nil)
    @swift = Swift.new(nil)

    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      @hadoop = Hadoop.new(@yml['Hadoop'])
    when 's3'
      @s3 = S3.new(@yml['S3'])
    when 'gcs'
      @gcs = GCS.new(@yml['GCS'])
    when 'swift'
      @swift = Swift.new(@yml['Swift'])
    when 'glusterfs'
      # Nothing to configure for glusterfs.
    else
      puts "ERROR: Unsupported ufs #{@yml['Type']}"
      exit(1)
    end
  end

  def type
    return @yml['Type']
  end

  def hadoop
    return @hadoop
  end

  def s3
    return @s3
  end

  def gcs
    return @gcs
  end
  
  def swift
    return @swift
  end

  def alluxio_dist(alluxio_version)
    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      return @hadoop.alluxio_dist(alluxio_version)
    when 's3'
      return @s3.alluxio_dist(alluxio_version)
    when 'gcs'
      return @gcs.alluxio_dist(alluxio_version)
    when 'swift'
      return @swift.alluxio_dist(alluxio_version)
    when 'glusterfs'
    # The base version should work for glusterfs
      return "alluxio-#{alluxio_version}-bin.tar.gz"
    else
      puts "ERROR: Unsupported ufs #{@yml['Type']}"
      exit(1)
    end
  end
end
