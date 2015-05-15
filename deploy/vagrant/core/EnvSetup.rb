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


def gen_ssh_key
  system "mkdir -p ./files && \
          if [ ! -f ./files/id_rsa ]; then ssh-keygen -f ./files/id_rsa -t rsa -N ''; fi"
end


def config_hosts(name)
  system 'mkdir', '-p', './files'
  # copy ./files/workers in Host to /tachyon/conf/workers in Vagrant
  file = File.open("./files/workers","w")
  for i in (1..Total)
    if i == Total 
      name[i] = "TachyonMaster"
    else
      name[i] = "TachyonWorker#{i}"
    end
    if file != nil
      file.write(name[i] + ".local"  + "\n")
    end
  end
  file.close unless file == nil
  
  if not (Provider == "openstack" or
          Provider == "docker" )
    hosts = File.open("files/hosts","w")
    for i in (1..Total)
      if i == Total 
        name[i] = "TachyonMaster"
      else
        name[i] = "TachyonWorker#{i}"
      end
      if hosts != nil
        hosts.write(Addr[i - 1] + " " + 
                    name[i] + ".local" + "\n")
      end
    end
    hosts.close unless hosts == nil
  end
end

require 'yaml'

# parse tachyon_version.yml
class TachyonVersion
  def initialize(yaml_path)
    puts 'parsing tachyon_version.yml'
    @yml = YAML.load_file(yaml_path)

    @type = @yml['Type']
    @repo = ''
    @version = ''
    @dist = ''
    case @type
    when "Local"
      puts 'using local tachyon dir'
    when "Github"
      @repo = @yml['Github']['Repo']
      @version = @yml['Github']['Version']
      puts "using github #{@repo}, version #{@version}"
    when "Release"
      @dist = @yml['Release']['Dist']
      puts "using tachyon dist #{@dist}"
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
    apache_url = "http://archive.apache.org/dist/hadoop/common/hadoop-%{Version}/hadoop-%{Version}.tar.gz"
    cdh_url = "https://repository.cloudera.com/cloudera/cloudera-repos/org/apache/hadoop/hadoop-dist/%{Version}/hadoop-dist-%{Version}.tar.gz"
    @url_template = {
      "apache" => apache_url,
      "cdh"    => cdh_url,
    }

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
  end

  def tarball_url
    return @url_template[@type] % {Version: @version}
  end

  def version
    return @version
  end

  def spark_profile
    return @spark_profile
  end
end

class UfsVersion
  def initialize(yaml_path)
    @yml = YAML.load_file(yaml_path)
    puts @yml

    @hadoop = HadoopVersion.new(nil)

    case @yml['Type']
    when 'hadoop1', 'hadoop2'
      @hadoop = HadoopVersion.new(@yml['Hadoop'])
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
end
