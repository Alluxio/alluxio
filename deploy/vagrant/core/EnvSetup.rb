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
    case @type
    when "Local"
      puts 'using local tachyon dir'
    when "Github"
      @repo = @yml['Github']['Repo']
      @version = @yml['Github']['Version']
      puts "using github #{@repo}, version #{@version}"
    else
      puts "Unknown VersionType, Only {Github | Local} supported"
      exit(1)
    end
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

    if @yml['Type'] == 'None'
      puts 'No Spark will be set up'
      @repo = ''
      @version = ''
      @v_lt_1 = true
      @use_spark = false
    else
      @use_spark = true
      @git = @yml['Github']
      @repo = @git['Repo']
      @version = @git['Version']
      @v_lt_1 = @git['Version_LessThan_1.0.0']
      puts "using github #{@repo}, version #{@version}"
    end
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

