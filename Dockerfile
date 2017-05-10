FROM centos:centos6

RUN yum install -y java-1.7.0-openjdk-devel
RUN yum install -y wget tar
RUN mkdir -p /opt/maven && cd /opt/maven && wget 'http://mirrors.ibiblio.org/apache/maven/maven-3/3.2.1/binaries/apache-maven-3.2.1-bin.tar.gz' && tar zxvf apache-maven-3.2.1-bin.tar.gz

## for docs
RUN yum install -y 'http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'
RUN yum install -y gcc g++ make automake autoconf curl-devel openssl-devel zlib-devel httpd-devel apr-devel 
RUN yum install -y apr-util-devel sqlite-devel which libyaml-devel libffi-devel gcc-c++ readline-devel libtool bison
RUN ln -sf /proc/self/fd /dev/fd
RUN curl -L get.rvm.io | bash -s stable
RUN source /etc/profile.d/rvm.sh && /usr/local/rvm/bin/rvm install 1.9.3
RUN wget 'http://production.cf.rubygems.org/rubygems/rubygems-2.4.1.tgz' && tar zxvf rubygems-2.4.1.tgz && cd rubygems-2.4.1 && source /etc/profile.d/rvm.sh && /usr/local/rvm/rubies/ruby-1.9.3-p547/bin/ruby setup.rb
# installs jekyll at /usr/local/rvm/rubies/ruby-1.9.3-p547/bin/jekyll
RUN /usr/local/rvm/rubies/ruby-1.9.3-p547/bin/gem install jekyll || ls -l /usr/local/rvm/rubies/ruby-1.9.3-p547/bin/jekyll
RUN yum install -y nodejs

ENV JAVA_HOME /usr/lib/jvm/jre-1.7.0-openjdk.x86_64
ENV M2_HOME /opt/maven/apache-maven-3.2.1
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/rvm/rubies/ruby-1.9.3-p547/bin
