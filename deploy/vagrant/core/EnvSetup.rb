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
    # puts "vagrant up #{current_provider}"
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

# parse zookeeper.yml
class ZookeeperVersion
  def initialize(yaml_path)
    puts 'parsing zookeeper.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @dist = ''
    case @type
    when "Release"
      @version = @yml['Release']['Version']
      puts "using zookeeper version #{@version}"
    when "None"
      puts 'No zookeeper will be set up'
    else
      puts "Unknown Type"
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

# parse alluxio_version.yml
class AlluxioVersion
  def initialize(yaml_path)
    puts 'parsing alluxio_version.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @repo = ''
    @version = ''
    case @type
    when "Local"
      puts 'using local alluxio dir'
    when "Github"
      @repo = @yml['Github']['Repo']
      @version = @yml['Github']['Version']
      puts "using github #{@repo}, version #{@version}"
    when "Release"
      @version = @yml['Release']['Version']
      puts "using alluxio version #{@version}"
    else
      puts "Unknown VersionType"
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

# parse mesos_version.yml
class MesosVersion
  def initialize(yaml_path)
    puts 'parsing mesos_version.yml'
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
      puts "using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "using mesos dist #{@dist}"
    when "None"
      puts 'No Mesos will be set up'
      @use_mesos = false
    else
      puts "Unknown VersionType"
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

# parse spark_version.yml
class SparkVersion
  def initialize(yaml_path)
    puts 'parsing spark_version.yml'
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
      puts "using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "using spark dist #{@dist}"
    else
      puts "Unknown VersionType"
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
    @type = ''
    @version = ''
    @spark_profile = ''
    if yml == nil
      return
    end
    @type = yml['Type']
    if @type == nil
      puts 'ERROR: Hadoop:Type is not set'
      exit(1)
    end
    @version = yml['Version']
    if @version == nil
      puts 'ERROR: Hadoop:Version is not set'
      exit(1)
    end
    @spark_profile = yml['SparkProfile'] or @spark_profile

    apache_url = "http://archive.apache.org/dist/hadoop/common/hadoop-%{Version}/hadoop-%{Version}.tar.gz"
    cdh_url = "https://repository.cloudera.com/cloudera/cloudera-repos/org/apache/hadoop/hadoop-dist/%{Version}/hadoop-dist-%{Version}.tar.gz"
    # cdh repository is different for cdh5
    if @version.include?('cdh5')
      cdh_url = "https://archive.cloudera.com/cdh5/cdh/5/hadoop-%{Version}.tar.gz"
    end
    @url_template = {
      "apache" => apache_url,
      "cdh"    => cdh_url,
    }

  end

  def tarball_url
    if @type == ''
      return ''
    else
      return @url_template[@type] % {Version: @version}
    end
  end

  def version
    return @version
  end

  def spark_profile
    return @spark_profile
  end

  def alluxio_dist(alluxio_version)
    # compute the alluxio distribution string
    errmsg = "ERROR: hadoop #{@type}-#{@version} does not have a " \
             "corresponding alluxio distribution"
    if @type == 'apache'
      if @version.start_with?('1')
        # It's the release distribution, so no suffix
        suffix = ''
      elsif @version.start_with?('2.4')
        suffix = 'hadoop2.4'
      elsif @version.start_with?('2.6')
        suffix = 'hadoop2.6'
      else
        puts errmsg
        exit(1)
      end
    elsif @type == 'cdh'
      if @version.start_with?('2') and @version.include?('cdh4')
        suffix = 'cdh4'
      elsif @version.start_with?('2') and @version.include?('cdh5')
        suffix = 'cdh5'
      else
        puts errmsg
        exit(1)
      end
    else
      puts "Unknown hadoop type #{@type}"
      exit(1)
    end
    if suffix.empty?
      return "alluxio-#{alluxio_version}-bin.tar.gz"
    else
      return "alluxio-#{alluxio_version}-#{suffix}-bin.tar.gz"
    end
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

class UfsVersion
  def get_default_ufs(provider)
    case provider
    when 'vb'
      puts 'use hadoop2 as default ufs'
      return 'hadoop2'
    when 'google'
      puts 'use hadoop2 as default ufs'
      return 'hadoop2'
    when 'aws'
      puts 'use s3 as default ufs'
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

    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      @hadoop = HadoopVersion.new(@yml['Hadoop'])
    when 's3'
      @s3 = S3Version.new(@yml['S3'])
    when 'glusterfs'
    else
      puts 'unsupported ufs'
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

  def alluxio_dist(alluxio_version)
    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      return @hadoop.alluxio_dist(alluxio_version)
    when 's3'
      return @s3.alluxio_dist(alluxio_version)
    when 'glusterfs'
    # The base version should work for glusterfs
      return "alluxio-#{alluxio_version}-bin.tar.gz"
    else
      puts 'unsupported ufs'
      exit(1)
    end
  end
end
