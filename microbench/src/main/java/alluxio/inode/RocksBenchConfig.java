package alluxio.inode;

import static org.apache.commons.io.FileUtils.writeStringToFile;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.master.metastore.rocks.DataBlockIndexType;
import alluxio.master.metastore.rocks.IndexType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class RocksBenchConfig {

  static final String JAVA_CONFIG = "javaConfig";
  static final String EMPTY_CONFIG = "emptyConfig";
  static final String BLOOM_CONFIG = "bloomConfig";
  static final String BASE_CONFIG = "baseConfig";

  static void setRocksConfig(String confType, String dir,
                             InstancedConfiguration conf) throws IOException {
    switch (confType) {
      case EMPTY_CONFIG:
        resetConfig(false, dir, conf);
        break;
      case JAVA_CONFIG:
        resetConfig(true, null, conf);
        break;
      case BLOOM_CONFIG:
        setBloomConfig(dir, conf);
        break;
      case BASE_CONFIG:
        setBaseConfig(dir, conf);
        break;
      default:
        throw new InvalidArgumentException(String.format(
            "Invalid RocksDB config type %s", confType));
    }
  }

  private static void resetConfig(boolean javaConfig,
      String dir, InstancedConfiguration conf) throws IOException {
    if (!javaConfig) {
      File confFile = new File(dir + "conf");
      writeStringToFile(confFile, NO_CONFIG_FILE, (Charset) null);
      conf.set(PropertyKey.ROCKS_INODE_CONF_FILE,
          (Object) confFile.getAbsolutePath());
    } else {
      // if the config file is unset, then the java configuration will be used
      conf.unset(PropertyKey.ROCKS_INODE_CONF_FILE);
    }
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOCK_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOOM_FILTER);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE);
  }

  private static void setBloomConfig(
      String dir, InstancedConfiguration conf) throws IOException {
    File confFile = new File(dir + "conf");
    writeStringToFile(confFile, BLOOM_CONFIG_FILE, (Charset) null);
    conf.set(PropertyKey.ROCKS_INODE_CONF_FILE,
        (Object) confFile.getAbsolutePath());
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinaryAndHash);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_INDEX, IndexType.kHashSearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOOM_FILTER, true);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE, 64 * 1024 * 1024);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinaryAndHash);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX, IndexType.kHashSearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER, true);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE, 64 * 1024 * 1024);
  }

  private static void setBaseConfig(String dir,
                            InstancedConfiguration conf) throws IOException {
    File confFile = new File(dir + "conf");
    writeStringToFile(confFile, BASE_CONFIG_FILE, (Charset) null);
    conf.set(PropertyKey.ROCKS_INODE_CONF_FILE,
        (Object) confFile.getAbsolutePath());
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_INDEX, IndexType.kBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOOM_FILTER, false);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE, 8 * 1024 * 1024);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX, IndexType.kBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER, false);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_CACHE_SIZE, 8 * 1024 * 1024);
  }

  private static final String NO_CONFIG_FILE = "\n"
      + "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  create_if_missing=true\n"
      + "  create_missing_column_families=true\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"default\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"default\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"inodes\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"inodes\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"edges\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"edges\"]\n"
      + "  \n";

  private static final String BASE_CONFIG_FILE = "\n"
      + "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  allow_concurrent_memtable_write=false\n"
      + "  create_if_missing=true\n"
      + "  create_missing_column_families=true\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"default\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"default\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"inodes\"]\n"
      + "   compression=kNoCompression\n"
      + "   prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "   memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"inodes\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"edges\"]\n"
      + "   compression=kNoCompression\n"
      + "   prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "   memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"edges\"]\n"
      + "  \n";

  private static final String BLOOM_CONFIG_FILE =  "\n"
      + "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  allow_concurrent_memtable_write=false\n"
      + "  create_if_missing=true\n"
      + "  create_missing_column_families=true\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"default\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"default\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"inodes\"]\n"
      + "  max_bytes_for_level_base=536870912\n"
      + "  memtable_whole_key_filtering=true\n"
      + "  max_write_buffer_number=6\n"
      + "  compression=kNoCompression\n"
      + "  level0_file_num_compaction_trigger=2\n"
      + "  prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "  write_buffer_size=134217728\n"
      + "  memtable_prefix_bloom_size_ratio=0.020000\n"
      + "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n"
      + "  min_write_buffer_number_to_merge=2      \n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"inodes\"]\n"
      + "  index_type=kHashSearch\n"
      + "  filter_policy=bloomfilter:10:false\n"
      + "  data_block_index_type=kDataBlockBinarySearch\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"edges\"]\n"
      + "  max_bytes_for_level_base=536870912\n"
      + "  memtable_whole_key_filtering=true\n"
      + "  max_write_buffer_number=6\n"
      + "  compression=kNoCompression\n"
      + "  level0_file_num_compaction_trigger=2\n"
      + "  prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "  write_buffer_size=134217728\n"
      + "  memtable_prefix_bloom_size_ratio=0.020000\n"
      + "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n"
      + "  min_write_buffer_number_to_merge=2      \n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"edges\"]\n"
      + "  index_type=kHashSearch\n"
      + "  filter_policy=bloomfilter:10:false\n"
      + "  data_block_index_type=kDataBlockBinarySearch\n"
      + "  \n";
}
