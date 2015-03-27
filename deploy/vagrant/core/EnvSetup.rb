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
  file = File.open("../../conf/workers","w")
  system 'mkdir', '-p', './files'
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

# Return Tachyon version defined in tachyon_version.yml, e.x. Github, Local
#
# If version is Github, generate core/download_tachyon.sh to 
# clone the repo
#
# If version is Local, remove core/download_tachyon.sh if it exists
def get_version
  @download_tachyon_sh = "core/download_tachyon.sh"

  @version = YAML.load_file('tachyon_version.yml')
  
  type = @version['Type']

  case type
  when "Local"
    puts 'using local tachyon dir'
    if File.exists? @download_tachyon_sh
      File.delete @download_tachyon_sh
    end
  when "Github"
    repo = @version['Github']['Repo']
    hash = @version['Github']['Hash']
    url = "#{repo}/commit/#{hash}"
    puts "using github commit #{url}"

    File.open(@download_tachyon_sh, "w") do |f|
      # script will be written to @download_tachyon_sh
      # and executed when Vagrant boots
      @script =
          "sudo yum install -y -q git\n"\
          "mkdir -p /tachyon\n"\
          "pushd /tachyon > /dev/null\n"\
          "git init\n"\
          "git remote add origin #{repo}\n"\
          "git fetch origin\n"\
          "git checkout #{hash}\n"\
          "cp /vagrant/files/workers /tachyon/conf/workers\n"\
          "popd > /dev/null"
      f.write(@script)
    end
  else
    puts "Unknown VersionType, Only {Github | Local} supported"
    exit(1)
  end
  
  return type
end
