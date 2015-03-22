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
require 'open-uri'

def get_tachyon_home
  @v = YAML.load_file('tachyon_version.yml')
  
  type = @v['Type']
  if type == "Local"
    puts 'using local tachyon dir'
    return "../../"
  end

  home = "../tachyon"
  if type == "Github"
    repo = @v['Github']['Repo']
    hash = @v['Github']['Hash']
    url = "#{repo}/commit/#{hash}"

    begin
      open(url)
    rescue OpenURI::HTTPError
      $stderr.print "The github URL #{url} is invalid"
      exit(1)
    end

    puts "git clone #{url} and mvn package"
    `if [ -d ../tachyon ]; then rm -rf ../tachyon; fi`
    `mkdir -p ../tachyon`
    `pushd ../tachyon > /dev/null; \
     git init; \
     git remote add origin #{repo}; \
     git fetch origin; \
     git checkout #{hash}; \
     mvn package; \
     popd > /dev/null`
    puts "done"
  else
    puts "Unknown VersionType, Only {Github | Local} supported"
    exit(1)
  end
end
