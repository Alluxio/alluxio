FROM centos:centos7
RUN yum -y update 
RUN yum install -y -q openssh-server wget systemd-sys sudo tar net-tools openssh-clients hostname
RUN useradd -c "vagrant" -d /home/vagrant -m -s /bin/bash -G wheel vagrant
RUN echo 'root:vagrant' | chpasswd
RUN echo 'vagrant:vagrant' | chpasswd
RUN mkdir -p /root/.ssh/ /home/vagrant/.ssh/
RUN wget --no-check-certificate https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant -O /root/.ssh/id_rsa
RUN wget --no-check-certificate https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant.pub -O /root/.ssh/id_rsa.pub
RUN wget --no-check-certificate https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant -O /home/vagrant/.ssh/id_rsa
RUN wget --no-check-certificate https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant.pub -O /home/vagrant/.ssh/id_rsa.pub
RUN chmod 600 /home/vagrant/.ssh/id*
RUN chmod 600 /root/.ssh/id*
RUN cp /root/.ssh/id_rsa /etc/ssh/ssh_host_key
RUN cp /root/.ssh/id_rsa /etc/ssh/ssh_host_rsa_key
RUN cp /root/.ssh/id_rsa /etc/ssh/ssh_host_ecdsa_key
RUN grep -v requiretty /etc/sudoers > /etc/sudoers2
RUN mv /etc/sudoers2 /etc/sudoers

RUN chmod 600 /root/.ssh/id*

#RUN /etc/init.d/sshd start
EXPOSE 22 
CMD /usr/sbin/sshd -D
