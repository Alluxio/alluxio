#1)编译项目
mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip   -Dcheckstyle.skip -Dlicense.skip -Dskip.protoc
#2)打包项目
#go env 参数一定要设置为auto
export GO111MODULE="auto"
./dev/scripts/generate-tarballs release
#3)打包成docker镜像
cd integration/docker/
docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-2.7.0-SNAPSHOT-bin.tar.gz  .