package alluxio.master.file.metasync;

public enum SyncFailReason {
  UNKNOWN,
  UFS_IO_FAILURE,
  CONCURRENT_UPDATE_DURING_SYNC
}
