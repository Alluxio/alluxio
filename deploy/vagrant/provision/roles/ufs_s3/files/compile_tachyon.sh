cd /tachyon
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true >mvn.log 2>&1
