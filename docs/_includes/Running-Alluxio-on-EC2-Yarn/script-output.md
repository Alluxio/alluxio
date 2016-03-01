```bash
Using $HADOOP_HOME set to '/hadoop'
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/alluxio/clients/client/target/alluxio-core-client-1.0.0-jar-with-dependencies.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Initializing Client
Starting Client
15/10/22 00:01:17 INFO client.RMProxy: Connecting to ResourceManager at AlluxioMaster/172.31.22.124:8050
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/alluxio/clients/client/target/alluxio-core-client-1.0.0-jar-with-dependencies.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
ApplicationMaster command: {{JAVA_HOME}}/bin/java -Xmx256M alluxio.yarn.ApplicationMaster 3 /alluxio localhost 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr 
Submitting application of id application_1445469376652_0002 to ResourceManager
15/10/22 00:01:19 INFO impl.YarnClientImpl: Submitted application application_1445469376652_0002
2015/10/22 00:01:29 Got application report from ASM for appId=2, clientToAMToken=null, appDiagnostics=, appMasterHost=, appQueue=default, appMasterRpcPort=0, appStartTime=1445472079196, yarnAppState=RUNNING, distributedFinalState=UNDEFINED, appTrackingUrl=http://AlluxioMaster:8088/proxy/application_1445469376652_0002/A, appUser=ec2-user
2015/10/22 00:01:39 Got application report from ASM for appId=2, clientToAMToken=null, appDiagnostics=, appMasterHost=, appQueue=default, appMasterRpcPort=0, appStartTime=1445472079196, yarnAppState=RUNNING, distributedFinalState=UNDEFINED, appTrackingUrl=http://AlluxioMaster:8088/proxy/application_1445469376652_0002/A, appUser=ec2-user
```
