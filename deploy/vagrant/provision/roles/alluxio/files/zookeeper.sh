ALLUXIO_SITE=/alluxio/conf/alluxio-site.properties
# For Alluxio version >= 0.8
echo "alluxio.zookeeper.enabled=true" >> "$ALLUXIO_SITE"
# For earlier Alluxio version
echo "alluxio.usezookeeper=true" >> "$ALLUXIO_SITE"

echo "alluxio.zookeeper.address=AlluxioMaster:2181" >> "$ALLUXIO_SITE"
