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

  static final String NO_CONFIG = "noConfig";
  static final String BLOOM_CONFIG = "bloomConfig";
  static final String BASE_CONFIG = "baseConfig";

  static void setRocksConfig(String confType, String dir,
                             InstancedConfiguration conf) throws IOException {
    switch (confType) {
      case NO_CONFIG:
        resetConfig(conf);
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

  private static void resetConfig(InstancedConfiguration conf) {
    conf.unset(PropertyKey.ROCKS_BLOCK_CONF_FILE);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOCK_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_EDGE_BLOOM_FILTER);
    conf.unset(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX);
    conf.unset(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER);
    conf.unset(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE);
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
    conf.set(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE, 64 * 1024 * 1024);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinaryAndHash);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX, IndexType.kHashSearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER, true);
    conf.set(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE, 64 * 1024 * 1024);
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
    conf.set(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE, 8 * 1024 * 1024);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOCK_INDEX,
        DataBlockIndexType.kDataBlockBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_INDEX, IndexType.kBinarySearch);
    conf.set(PropertyKey.MASTER_METASTORE_ROCKS_INODE_BLOOM_FILTER, false);
    conf.set(PropertyKey.MATER_METASTORE_ROCKS_INODE_CACHE_SIZE, 8 * 1024 * 1024);
  }

  private static final String BASE_CONFIG_FILE = "\n" +
      "[Version]\n" +
      "  rocksdb_version=7.0.3\n" +
      "  options_file_version=1.1\n" +
      "\n" +
      "[DBOptions]\n" +
      "  max_background_flushes=-1\n" +
      "  compaction_readahead_size=0\n" +
      "  strict_bytes_per_sync=false\n" +
      "  wal_bytes_per_sync=0\n" +
      "  max_open_files=-1\n" +
      "  stats_history_buffer_size=1048576\n" +
      "  max_total_wal_size=0\n" +
      "  stats_persist_period_sec=600\n" +
      "  stats_dump_period_sec=600\n" +
      "  avoid_flush_during_shutdown=false\n" +
      "  max_subcompactions=1\n" +
      "  bytes_per_sync=0\n" +
      "  delayed_write_rate=16777216\n" +
      "  max_background_compactions=-1\n" +
      "  max_background_jobs=2\n" +
      "  delete_obsolete_files_period_micros=21600000000\n" +
      "  writable_file_max_buffer_size=1048576\n" +
      "  file_checksum_gen_factory=nullptr\n" +
      "  allow_data_in_errors=false\n" +
      "  rate_limiter=nullptr\n" +
      "  max_bgerror_resume_count=2147483647\n" +
      "  best_efforts_recovery=false\n" +
      "  write_dbid_to_manifest=false\n" +
      "  atomic_flush=false\n" +
      "  wal_compression=kNoCompression\n" +
      "  manual_wal_flush=false\n" +
      "  two_write_queues=false\n" +
      "  avoid_flush_during_recovery=false\n" +
      "  dump_malloc_stats=false\n" +
      "  info_log_level=INFO_LEVEL\n" +
      "  access_hint_on_compaction_start=NORMAL\n" +
      "  write_thread_slow_yield_usec=3\n" +
      "  bgerror_resume_retry_interval=1000000\n" +
      "  paranoid_checks=true\n" +
      "  WAL_size_limit_MB=0\n" +
      "  allow_concurrent_memtable_write=false\n" +
      "  allow_ingest_behind=false\n" +
      "  fail_if_options_file_error=false\n" +
      "  persist_stats_to_disk=false\n" +
      "  WAL_ttl_seconds=0\n" +
      "  lowest_used_cache_tier=kNonVolatileBlockTier\n" +
      "  keep_log_file_num=1000\n" +
      "  table_cache_numshardbits=6\n" +
      "  max_file_opening_threads=16\n" +
      "  use_fsync=false\n" +
      "  unordered_write=false\n" +
      "  random_access_max_buffer_size=1048576\n" +
      "  log_readahead_size=0\n" +
      "  enable_pipelined_write=false\n" +
      "  wal_recovery_mode=kPointInTimeRecovery\n" +
      "  db_write_buffer_size=0\n" +
      "  allow_2pc=false\n" +
      "  skip_checking_sst_file_sizes_on_db_open=false\n" +
      "  skip_stats_update_on_db_open=false\n" +
      "  track_and_verify_wals_in_manifest=false\n" +
      "  error_if_exists=false\n" +
      "  manifest_preallocation_size=4194304\n" +
      "  is_fd_close_on_exec=true\n" +
      "  enable_write_thread_adaptive_yield=true\n" +
      "  experimental_mempurge_threshold=0.000000\n" +
      "  enable_thread_tracking=false\n" +
      "  avoid_unnecessary_blocking_io=false\n" +
      "  allow_fallocate=true\n" +
      "  max_log_file_size=0\n" +
      "  advise_random_on_open=true\n" +
      "  create_missing_column_families=true\n" +
      "  max_write_batch_group_size_bytes=1048576\n" +
      "  use_adaptive_mutex=false\n" +
      "  wal_filter=nullptr\n" +
      "  create_if_missing=true\n" +
      "  allow_mmap_writes=false\n" +
      "  log_file_time_to_roll=0\n" +
      "  use_direct_io_for_flush_and_compaction=false\n" +
      "  flush_verify_memtable_count=true\n" +
      "  max_manifest_file_size=1073741824\n" +
      "  write_thread_max_yield_usec=100\n" +
      "  use_direct_reads=false\n" +
      "  recycle_log_file_num=0\n" +
      "  db_host_id=__hostname__\n" +
      "  allow_mmap_reads=false\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"default\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=268435456\n" +
      "  memtable_whole_key_filtering=false\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=2\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kSnappyCompression\n" +
      "  level0_file_num_compaction_trigger=4\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=nullptr\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=67108864\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.000000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory=SkipListFactory\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=1\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"default\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kBinarySearch\n" +
      "  filter_policy=nullptr\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinarySearch\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"inodes\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=268435456\n" +
      "  memtable_whole_key_filtering=false\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=2\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kNoCompression\n" +
      "  level0_file_num_compaction_trigger=4\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=rocksdb.FixedPrefix.8\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=67108864\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.000000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=1\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"inodes\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kBinarySearch\n" +
      "  filter_policy=nullptr\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinarySearch\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"edges\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=268435456\n" +
      "  memtable_whole_key_filtering=false\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=2\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kNoCompression\n" +
      "  level0_file_num_compaction_trigger=4\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=rocksdb.FixedPrefix.8\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=67108864\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.000000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=1\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"edges\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kBinarySearch\n" +
      "  filter_policy=nullptr\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinarySearch\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n";

  private static final String BLOOM_CONFIG_FILE = "# This is a RocksDB option file.\n" +
      "#\n" +
      "# For detailed file format spec, please refer to the example file\n" +
      "# in examples/rocksdb_option_file_example.ini\n" +
      "#\n" +
      "\n" +
      "[Version]\n" +
      "  rocksdb_version=7.0.3\n" +
      "  options_file_version=1.1\n" +
      "\n" +
      "[DBOptions]\n" +
      "  max_background_flushes=-1\n" +
      "  compaction_readahead_size=0\n" +
      "  strict_bytes_per_sync=false\n" +
      "  wal_bytes_per_sync=0\n" +
      "  max_open_files=-1\n" +
      "  stats_history_buffer_size=1048576\n" +
      "  max_total_wal_size=0\n" +
      "  stats_persist_period_sec=600\n" +
      "  stats_dump_period_sec=600\n" +
      "  avoid_flush_during_shutdown=false\n" +
      "  max_subcompactions=1\n" +
      "  bytes_per_sync=0\n" +
      "  delayed_write_rate=16777216\n" +
      "  max_background_compactions=-1\n" +
      "  max_background_jobs=2\n" +
      "  delete_obsolete_files_period_micros=21600000000\n" +
      "  writable_file_max_buffer_size=1048576\n" +
      "  file_checksum_gen_factory=nullptr\n" +
      "  allow_data_in_errors=false\n" +
      "  rate_limiter=nullptr\n" +
      "  max_bgerror_resume_count=2147483647\n" +
      "  best_efforts_recovery=false\n" +
      "  write_dbid_to_manifest=false\n" +
      "  atomic_flush=false\n" +
      "  wal_compression=kNoCompression\n" +
      "  manual_wal_flush=false\n" +
      "  two_write_queues=false\n" +
      "  avoid_flush_during_recovery=false\n" +
      "  dump_malloc_stats=false\n" +
      "  info_log_level=INFO_LEVEL\n" +
      "  access_hint_on_compaction_start=NORMAL\n" +
      "  write_thread_slow_yield_usec=3\n" +
      "  bgerror_resume_retry_interval=1000000\n" +
      "  paranoid_checks=true\n" +
      "  WAL_size_limit_MB=0\n" +
      "  allow_concurrent_memtable_write=false\n" +
      "  allow_ingest_behind=false\n" +
      "  fail_if_options_file_error=false\n" +
      "  persist_stats_to_disk=false\n" +
      "  WAL_ttl_seconds=0\n" +
      "  lowest_used_cache_tier=kNonVolatileBlockTier\n" +
      "  keep_log_file_num=1000\n" +
      "  table_cache_numshardbits=6\n" +
      "  max_file_opening_threads=16\n" +
      "  use_fsync=false\n" +
      "  unordered_write=false\n" +
      "  random_access_max_buffer_size=1048576\n" +
      "  log_readahead_size=0\n" +
      "  enable_pipelined_write=false\n" +
      "  wal_recovery_mode=kPointInTimeRecovery\n" +
      "  db_write_buffer_size=0\n" +
      "  allow_2pc=false\n" +
      "  skip_checking_sst_file_sizes_on_db_open=false\n" +
      "  skip_stats_update_on_db_open=false\n" +
      "  track_and_verify_wals_in_manifest=false\n" +
      "  error_if_exists=false\n" +
      "  manifest_preallocation_size=4194304\n" +
      "  is_fd_close_on_exec=true\n" +
      "  enable_write_thread_adaptive_yield=true\n" +
      "  experimental_mempurge_threshold=0.000000\n" +
      "  enable_thread_tracking=false\n" +
      "  avoid_unnecessary_blocking_io=false\n" +
      "  allow_fallocate=true\n" +
      "  max_log_file_size=0\n" +
      "  advise_random_on_open=true\n" +
      "  create_missing_column_families=true\n" +
      "  max_write_batch_group_size_bytes=1048576\n" +
      "  use_adaptive_mutex=false\n" +
      "  wal_filter=nullptr\n" +
      "  create_if_missing=true\n" +
      "  allow_mmap_writes=false\n" +
      "  log_file_time_to_roll=0\n" +
      "  use_direct_io_for_flush_and_compaction=false\n" +
      "  flush_verify_memtable_count=true\n" +
      "  max_manifest_file_size=1073741824\n" +
      "  write_thread_max_yield_usec=100\n" +
      "  use_direct_reads=false\n" +
      "  recycle_log_file_num=0\n" +
      "  db_host_id=__hostname__\n" +
      "  allow_mmap_reads=false\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"default\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=268435456\n" +
      "  memtable_whole_key_filtering=false\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=2\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kSnappyCompression\n" +
      "  level0_file_num_compaction_trigger=4\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=nullptr\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=67108864\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.000000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory=SkipListFactory\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=1\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"default\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kBinarySearch\n" +
      "  filter_policy=nullptr\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinarySearch\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"inodes\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=536870912\n" +
      "  memtable_whole_key_filtering=true\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=6\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kNoCompression\n" +
      "  level0_file_num_compaction_trigger=2\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=rocksdb.FixedPrefix.8\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=134217728\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.020000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=2\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  compression_per_level=kNoCompression:kNoCompression:kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"inodes\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kHashSearch\n" +
      "  filter_policy=bloomfilter:10:false\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinaryAndHash\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n" +
      "\n" +
      "[CFOptions \"edges\"]\n" +
      "  bottommost_compression=kDisableCompressionOption\n" +
      "  sample_for_compression=0\n" +
      "  blob_garbage_collection_age_cutoff=0.250000\n" +
      "  blob_compaction_readahead_size=0\n" +
      "  level0_stop_writes_trigger=36\n" +
      "  min_blob_size=0\n" +
      "  compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;compression_size_percent=-1;max_size_amplification_percent=200;incremental=false;max_merge_width=4294967295;size_ratio=1;}\n" +
      "  target_file_size_base=67108864\n" +
      "  max_bytes_for_level_base=536870912\n" +
      "  memtable_whole_key_filtering=true\n" +
      "  soft_pending_compaction_bytes_limit=68719476736\n" +
      "  blob_compression_type=kNoCompression\n" +
      "  max_write_buffer_number=6\n" +
      "  ttl=2592000\n" +
      "  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;}\n" +
      "  check_flush_compaction_key_order=true\n" +
      "  max_successive_merges=0\n" +
      "  inplace_update_num_locks=10000\n" +
      "  enable_blob_garbage_collection=false\n" +
      "  arena_block_size=1048576\n" +
      "  bottommost_temperature=kUnknown\n" +
      "  bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  target_file_size_multiplier=1\n" +
      "  max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1\n" +
      "  blob_garbage_collection_force_threshold=1.000000\n" +
      "  enable_blob_files=false\n" +
      "  level0_slowdown_writes_trigger=20\n" +
      "  compression=kNoCompression\n" +
      "  level0_file_num_compaction_trigger=2\n" +
      "  blob_file_size=268435456\n" +
      "  prefix_extractor=rocksdb.FixedPrefix.8\n" +
      "  max_bytes_for_level_multiplier=10.000000\n" +
      "  write_buffer_size=134217728\n" +
      "  disable_auto_compactions=false\n" +
      "  max_compaction_bytes=1677721600\n" +
      "  memtable_huge_page_size=0\n" +
      "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n" +
      "  hard_pending_compaction_bytes_limit=274877906944\n" +
      "  periodic_compaction_seconds=0\n" +
      "  paranoid_file_checks=false\n" +
      "  memtable_prefix_bloom_size_ratio=0.020000\n" +
      "  max_sequential_skip_in_iterations=8\n" +
      "  report_bg_io_stats=false\n" +
      "  sst_partitioner_factory=nullptr\n" +
      "  compaction_pri=kMinOverlappingRatio\n" +
      "  compaction_style=kCompactionStyleLevel\n" +
      "  compaction_filter_factory=nullptr\n" +
      "  memtable_factory={id=HashSkipListRepFactory;branching_factor=4;skiplist_height=4;bucket_count=1000000;}\n" +
      "  comparator=leveldb.BytewiseComparator\n" +
      "  bloom_locality=0\n" +
      "  min_write_buffer_number_to_merge=2\n" +
      "  max_write_buffer_number_to_maintain=0\n" +
      "  compaction_filter=nullptr\n" +
      "  merge_operator=nullptr\n" +
      "  compression_per_level=kNoCompression:kNoCompression:kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression\n" +
      "  num_levels=7\n" +
      "  force_consistency_checks=true\n" +
      "  optimize_filters_for_hits=false\n" +
      "  table_factory=BlockBasedTable\n" +
      "  max_write_buffer_size_to_maintain=0\n" +
      "  memtable_insert_with_hint_prefix_extractor=nullptr\n" +
      "  level_compaction_dynamic_level_bytes=false\n" +
      "  inplace_update_support=false\n" +
      "  \n" +
      "[TableOptions/BlockBasedTable \"edges\"]\n" +
      "  metadata_cache_options={unpartitioned_pinning=kFallback;partition_pinning=kFallback;top_level_index_pinning=kFallback;}\n" +
      "  read_amp_bytes_per_bit=0\n" +
      "  verify_compression=false\n" +
      "  format_version=5\n" +
      "  optimize_filters_for_memory=false\n" +
      "  partition_filters=false\n" +
      "  detect_filter_construct_corruption=false\n" +
      "  max_auto_readahead_size=262144\n" +
      "  enable_index_compression=true\n" +
      "  checksum=kCRC32c\n" +
      "  index_block_restart_interval=1\n" +
      "  pin_top_level_index_and_filter=true\n" +
      "  block_align=false\n" +
      "  block_size=4096\n" +
      "  index_type=kHashSearch\n" +
      "  filter_policy=bloomfilter:10:false\n" +
      "  metadata_block_size=4096\n" +
      "  no_block_cache=false\n" +
      "  index_shortening=kShortenSeparators\n" +
      "  reserve_table_builder_memory=false\n" +
      "  whole_key_filtering=true\n" +
      "  block_size_deviation=10\n" +
      "  data_block_index_type=kDataBlockBinaryAndHash\n" +
      "  data_block_hash_table_util_ratio=0.750000\n" +
      "  cache_index_and_filter_blocks=false\n" +
      "  prepopulate_block_cache=kDisable\n" +
      "  block_restart_interval=16\n" +
      "  pin_l0_filter_and_index_blocks_in_cache=false\n" +
      "  hash_index_allow_collision=true\n" +
      "  cache_index_and_filter_blocks_with_high_priority=true\n" +
      "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n" +
      "  \n";
}
