package alluxio.conf;

import alluxio.Constants;
import alluxio.grpc.Scope;

/**
 * Oh noe!!.
 */
public class RemovedKey {

  @Removed(message = "The alluxio web UI overhaul in v2.0 removed this parameter.")
  public static final PropertyKey WEB_TEMP_PATH =
      new PropertyKey.Builder(PropertyKey.Name.WEB_TEMP_PATH)
          .setDefaultValue(String.format("${%s}/web/", PropertyKey.Name.WORK_DIR))
          .setDescription("Path to store temporary web server files.")
          .setScope(Scope.SERVER)
          .build();

  @Removed
  public static final PropertyKey UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
      new PropertyKey.Builder(PropertyKey.Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS)
          .setAlias(new String[]{"alluxio.underfs.s3a.consistency.timeout.ms"})
          .setDefaultValue("1min")
          .setDescription("The duration to wait for metadata consistency from the under "
              + "storage. This is only used by internal Alluxio operations which should be "
              + "successful, but may appear unsuccessful due to eventual consistency.")
          .setScope(Scope.SERVER)
          .build();

  @Removed(message = "It is no longer used in v2.0 and later")
  public static final PropertyKey SWIFT_USE_PUBLIC_URI_KEY =
      new PropertyKey.Builder(PropertyKey.Name.SWIFT_USE_PUBLIC_URI_KEY)
          .setDescription("Whether the REST server is in a public domain: true (default) or false.")
          .build();

  @Removed(message = "v2.0 removed the ability to configure this parameter")
  public static final PropertyKey MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL =
      new PropertyKey.Builder(PropertyKey.Name.MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL)
          .setDefaultValue("10min")
          .setDescription("Interval for removing closed client sockets from internal tracking.")
          .setIsHidden(true)
          .setScope(Scope.MASTER)
          .build();

  @Removed(message = "v2.0 removed the ability to configure this parameter")
  public static final PropertyKey MASTER_ACTIVE_UFS_SYNC_RETRY_TIMEOUT =
      new PropertyKey.Builder(PropertyKey.Name.MASTER_ACTIVE_UFS_SYNC_RETRY_TIMEOUT)
          .setDescription("Retry period before active ufs syncer gives up on connecting to the ufs")
          .setDefaultValue("1hour")
          .setScope(Scope.MASTER)
          .build();

  @Removed(message = "v2.0 removed the ability to configure this parameter")
  public static final PropertyKey MASTER_ACTIVE_UFS_SYNC_BATCH_INTERVAL =
      new PropertyKey.Builder(PropertyKey.Name.MASTER_ACTIVE_UFS_SYNC_BATCH_INTERVAL)
          .setDefaultValue("1sec")
          .setDescription("Time interval to batch incoming events for active syncing UFS")
          .setScope(Scope.MASTER)
          .build();

  @Removed(message = "v2.0 removed the ability to specify the master journal formatter")
  public static final PropertyKey MASTER_JOURNAL_FORMATTER_CLASS =
      new PropertyKey.Builder(PropertyKey.Name.MASTER_JOURNAL_FORMATTER_CLASS)
          .setDefaultValue("alluxio.master.journalv0.ProtoBufJournalFormatter")
          .setDescription("The class to serialize the journal in a specified format.")
          .setScope(Scope.MASTER)
          .build();

  @Removed(message = "v2.0 removed the ability to configure this parameter")
  public static final PropertyKey LOGSERVER_LOGS_DIR =
      new PropertyKey.Builder(PropertyKey.Name.LOGSERVER_LOGS_DIR)
          .setDefaultValue(String.format("${%s}/logs", PropertyKey.Name.WORK_DIR))
          .setDescription("Default location for remote log files.")
          .setIgnoredSiteProperty(true)
          .setScope(Scope.SERVER)
          .build();

  @Removed(message = "v2.0 removed the ability to configure this parameter")
  public static final PropertyKey LOGSERVER_HOSTNAME =
      new PropertyKey.Builder(PropertyKey.Name.LOGSERVER_HOSTNAME)
          .setDescription("The hostname of Alluxio logserver.")
          .setIgnoredSiteProperty(true)
          .setScope(Scope.SERVER)
          .build();

  @Removed(message = "v2.0 removed this property key. Use alluxio.locality.node instead to set"
      + " the client hostname.")
  public static final PropertyKey USER_HOSTNAME = new PropertyKey.Builder(PropertyKey.Name.USER_HOSTNAME)
      .setDescription("The hostname to use for the client.")
      .setScope(Scope.CLIENT)
      .build();

  @Removed
  public static final PropertyKey USER_RPC_RETRY_MAX_NUM_RETRY =
      new PropertyKey.Builder(PropertyKey.Name.USER_RPC_RETRY_MAX_NUM_RETRY)
          .setAlias(new String[]{PropertyKey.Name.MASTER_RETRY})
          .setDefaultValue(100)
          .setDescription("Alluxio client RPCs automatically retry for transient errors with "
              + "an exponential backoff. This property determines the maximum number of "
              + "retries. This property has been deprecated by time-based retry using: "
              + PropertyKey.Name.USER_RPC_RETRY_MAX_DURATION)
          .setScope(Scope.CLIENT)
          .build();

  private static final class Name {
  }
}
