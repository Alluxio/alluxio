# -*- mode: ruby -*-
# vi: set ft=ruby :

def validate_provider(provider)
  current_provider = nil
  if ARGV[0] and ARGV[0] == "up"
    if ARGV[1] and \
      (ARGV[1].split('=')[0] == "--provider" or ARGV[2])
      current_provider = (ARGV[1].split('=')[1] || ARGV[2])
    else
      current_provider = (ENV['VAGRANT_DEFAULT_PROVIDER'] || :virtualbox).to_s
    end
    if ( provider == current_provider )
      return
    end

    if ("virtualbox" == current_provider and provider == "vb")
      return
    end

    raise "\nMISMATCH FOUND\nProvider in init.yml is #{provider}." +
          "\nBut vagrant provider is #{current_provider}."
  end
end


require 'yaml'

class ZookeeperVersion
  def initialize(yaml_path)
    puts 'Parsing zookeeper.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @dist = ''
    case @type
    when "Release"
      @version = @yml['Release']['Version']
      puts "Using zookeeper version #{@version}"
    when "None"
      puts 'No zookeeper will be set up'
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

class AlluxioVersion
  def initialize(yaml_path)
    puts 'Parsing alluxio.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @repo = ''
    @version = ''
    major, minor = nil
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
      puts "Using alluxio version #{@version}"
    else
      puts "ERROR: Unknown Alluxio type #{@type}"
      exit(1)
    end

    # Determine if the version is less than 1.1, only for release and github release branch types
    major = Integer(major) rescue nil
    minor = Integer(minor) rescue nil
    @v_lt_0_8 = false
    @v_lt_1_1 = false
    if not major.nil? and not minor.nil?
      @v_lt_0_8 = ((major == 0) and (minor < 8))
      @v_lt_1_1 = ((major < 1) or (major == 1 and minor < 1))
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

  def v_lt_0_8
    return @v_lt_0_8
  end

  def v_lt_1_1
    return @v_lt_1_1
  end
end

class MesosVersion
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
      puts "Using mesos distribution #{@dist}"
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

class SparkVersion
  def initialize(yaml_path)
    puts 'Parsing spark.yml'
    @yml = YAML.load_file(yaml_path)

    @use_spark = true
    @repo = ''
    @version = ''
    @dist = ''
    @v_lt_1 = false
    case @yml['Type']
    when "None"
      puts 'No Spark will be set up'
      @use_spark = false
    when "Github"
      @git = @yml['Github']
      @repo = @git['Repo']
      @version = @git['Version']
      @v_lt_1 = @git['Version_LessThan_1.0.0']
      puts "Using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "Using spark distribution #{@dist}"
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

  def v_lt_1
    return @v_lt_1
  end
end

class HadoopVersion
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
		hadoop_major_minor_version = @version.split('.')[0..1].join('.')
    return "alluxio-#{alluxio_version}-hadoop-#{hadoop_major_minor_version}-bin.tar.gz"
  end
end

class S3Version
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

class SwiftVersion
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

class GCSVersion
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

class UfsVersion
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

    @hadoop = HadoopVersion.new(nil)
    @s3 = S3Version.new(nil)
    @gcs = GCSVersion.new(nil)
    @swift = SwiftVersion.new(nil)

    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      @hadoop = HadoopVersion.new(@yml['Hadoop'])
    when 's3'
      @s3 = S3Version.new(@yml['S3'])
    when 'gcs'
      @gcs = GCSVersion.new(@yml['GCS'])
    when 'swift'
      @swift = SwiftVersion.new(@yml['Swift'])
    when 'glusterfs'
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
