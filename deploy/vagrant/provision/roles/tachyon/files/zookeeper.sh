TACHYON_SITE=/tachyon/conf/tachyon-site.properties
# For Tachyon version >= 0.8
echo "alluxio.zookeeper.enabled=true" >> "$TACHYON_SITE"
# For earlier Tachyon version
echo "alluxio.usezookeeper=true" >> "$TACHYON_SITE"

echo "alluxio.zookeeper.address=TachyonMaster:2181" >> "$TACHYON_SITE"
