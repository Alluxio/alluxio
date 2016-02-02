 ```
 [ERROR] Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.2:compile (default-compile) on project tachyon-integration-yarn: Compilation failure: Compilation failure:
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[273,49] cannot find symbol
 [ERROR] symbol:   method $$()
 [ERROR] location: variable JAVA_HOME of type org.apache.hadoop.yarn.api.ApplicationConstants.Environment
 [ERROR] /Work/tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[307,31] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[310,29] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[312,47] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] /tachyon/upstream/integration/yarn/src/main/java/tachyon/yarn/Client.java:[314,47] cannot find symbol
 [ERROR] symbol:   variable CLASS_PATH_SEPARATOR
 [ERROR] location: interface org.apache.hadoop.yarn.api.ApplicationConstants
 [ERROR] -> [Help 1]
 ```
