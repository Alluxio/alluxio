package alluxio.master.file.metasync;

public enum SyncOperation {
  NOOP,
  CREATE,
  DELETE,
  RECREATE,
  UPDATE,
  SKIPPED_DUE_TO_CONCURRENT_MODIFICATION,
}
