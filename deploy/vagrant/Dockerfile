FROM centos:centos6
RUN yum -y update 
RUN yum install -y openssh-server wget sudo openssh-clients tar which
RUN useradd -c "vagrant" -d /home/vagrant -m -s /bin/bash -G wheel vagrant
RUN echo 'root:vagrant' | chpasswd
RUN echo 'vagrant:vagrant' | chpasswd
RUN mkdir -p /root/.ssh/ /home/vagrant/.ssh/

#RUN cp /vagrant/files/* /root/.ssh/
#RUN cp /vagrant/files/* /home/vagrant/.ssh/
#RUN chmod 600 /home/vagrant/.ssh/*
#RUN chmod 600 /root/.ssh/*

RUN grep -v requiretty /etc/sudoers > /etc/sudoers2
RUN mv /etc/sudoers2 /etc/sudoers

RUN /etc/init.d/sshd start

RUN echo "CREATED /shared"
EXPOSE 22 

# Required: A default command for docker run.
CMD /usr/sbin/sshd -D
