#!/usr/bin/env bash

mysql_install_db --user=mysql --datadir=/var/lib/mysql

# configure mysql
cat <<EOF >> /etc/my.cnf
skip-networking=OFF
bind-address=0.0.0.0
EOF

# start mysql
/usr/bin/mysqld_safe &
sleep 2

# create root user
mysqladmin -u root password "root"
echo "CREATE USER 'root'@'%'; GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;" | mysql -uroot -proot

# configure hive metastore to access the local mysql
cat <<EOF > /opt/hive/conf/hive-site.xml
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://localhost:9083</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true</value>
    <description>JDBC connect string for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>root</value>
    <description>username to use against metastore database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>root</value>
    <description>password to use against metastore database</description>
  </property>
</configuration>
EOF

# initialize hive metastore
cd /opt/hive/ && ./bin/schematool -dbType mysql -initSchema --verbose

# stop mysql
killall mysqld
