FROM centos:centos7
RUN yum -y update && \
    yum install -y -q openssh-server systemd-sys sudo tar net-tools openssh-clients hostname && \
    useradd -c "vagrant" -d /home/vagrant -m -s /bin/bash -G wheel vagrant && \
    echo 'root:vagrant' | chpasswd && \
    echo 'vagrant:vagrant' | chpasswd && \
    mkdir -p /root/.ssh/ && \
    mkdir -p /home/vagrant/.ssh/ 

ADD files/id_rsa /root/.ssh/id_rsa
ADD files/id_rsa.pub /root/.ssh/id_rsa.pub
ADD files/id_rsa /home/vagrant/.ssh/id_rsa
ADD files/id_rsa.pub /home/vagrant/.ssh/id_rsa.pub

RUN chmod 600 /home/vagrant/.ssh/id* && \
    chmod 600 /root/.ssh/id* && \
    cp /root/.ssh/id_rsa /etc/ssh/ssh_host_key && \
    cp /root/.ssh/id_rsa /etc/ssh/ssh_host_rsa_key && \
    cp /root/.ssh/id_rsa /etc/ssh/ssh_host_ecdsa_key && \
    grep -v requiretty /etc/sudoers > /etc/sudoers2 && \
    mv /etc/sudoers2 /etc/sudoers

EXPOSE 22 
CMD /usr/sbin/sshd -D
