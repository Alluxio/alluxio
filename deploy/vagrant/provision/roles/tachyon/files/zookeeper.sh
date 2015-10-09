TACHYON_SITE=/tachyon/conf/tachyon-site.properties
# For Tachyon version >= 0.8
echo "tachyon.zookeeper.enabled=true" >> "$TACHYON_SITE"
# For earlier Tachyon version
echo "tachyon.usezookeeper=true" >> "$TACHYON_SITE"

echo "tachyon.zookeeper.address=TachyonMaster:2181" >> "$TACHYON_SITE"
