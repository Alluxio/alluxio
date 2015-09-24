TACHYON_SITE=/tachyon/conf/tachyon-site.properties
echo "tachyon.usezookeeper=true" >> "$TACHYON_SITE"
echo "tachyon.zookeeper.address=TachyonMaster:2181" >> "$TACHYON_SITE"
