/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * This class contains old {@link PropertyKey}s which have been removed from use
 *
 * This class is used to track keys which were deprecated in previous versions and subsequently
 * removed in a future version. We still keep them here so that it is possible to provide users
 * with useful information if they are found to be using an outdated property key.
 *
 * Being removed and still used by an application denotes an error.
 *
 * @see InstancedConfiguration#validate()
 * @see PropertyKey#fromString(String)
 */
public class RemovedKey {

  private static final String V2_0_0_REMOVED = "this parameter is removed since v2.0.0.";

  private static final Map<String, String> REMOVED_KEYS = new HashMap<String, String>(20) {
    {
      put(Name.WEB_TEMP_PATH, "The alluxio web UI overhaul in v2.0 removed this parameter.");
      put(Name.UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS, V2_0_0_REMOVED);
      put(Name.SWIFT_USE_PUBLIC_URI_KEY, V2_0_0_REMOVED);
      put(Name.MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL, V2_0_0_REMOVED);
      put(Name.MASTER_CONNECTION_TIMEOUT, V2_0_0_REMOVED);
      put(Name.MASTER_FILE_ASYNC_PERSIST_HANDLER, V2_0_0_REMOVED);
      put(Name.MASTER_HEARTBEAT_INTERVAL, V2_0_0_REMOVED + " Use "
          + PropertyKey.Name.MASTER_WORKER_HEARTBEAT_INTERVAL + " instead.");
      put(Name.MASTER_JOURNAL_FORMATTER_CLASS, "v2.0 removed the ability to specify the master "
          + "journal formatter");
      put(Name.SWIFT_API_KEY, V2_0_0_REMOVED + " Use " + PropertyKey.Name.SWIFT_PASSWORD_KEY + " "
          + "instead.");
      put(Name.USER_FAILED_SPACE_REQUEST_LIMITS, V2_0_0_REMOVED);
      put(Name.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, V2_0_0_REMOVED);
      put(Name.USER_FILE_SEEK_BUFFER_SIZE_BYTES, V2_0_0_REMOVED);
      put(Name.USER_HOSTNAME, V2_0_0_REMOVED + " Use "
          + PropertyKey.Template.LOCALITY_TIER.format("node")
          + " instead to set the client hostname.");
      put(Name.USER_NETWORK_SOCKET_TIMEOUT, V2_0_0_REMOVED);
      put("alluxio.security.authentication.socket.timeout", V2_0_0_REMOVED);
      put("alluxio.security.authentication.socket.timeout.ms", V2_0_0_REMOVED);
      put(Name.USER_RPC_RETRY_MAX_NUM_RETRY, V2_0_0_REMOVED);
      put(Name.MASTER_RETRY, V2_0_0_REMOVED);
      put(Name.USER_BLOCK_WORKER_CLIENT_THREADS, V2_0_0_REMOVED);
      put(Name.USER_HEARTBEAT_INTERVAL, V2_0_0_REMOVED);
      put(Name.USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES, V2_0_0_REMOVED);
      put(Name.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES, V2_0_0_REMOVED);
      put(Name.WORKER_BLOCK_THREADS_MAX, V2_0_0_REMOVED);
      put(Name.WORKER_BLOCK_THREADS_MIN, V2_0_0_REMOVED);
      put(Name.WORKER_DATA_BIND_HOST, V2_0_0_REMOVED);
      put(Name.WORKER_DATA_PORT, V2_0_0_REMOVED);
      put(Name.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO, "Use alluxio.worker.tieredstore.levelX"
          + ".watermark.{high/low}.ratio instead");
      put(Name.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO, "Use alluxio.worker.tieredstore.levelX"
          + ".watermark.{high/low}.ratio instead");
      put(Name.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO, "Use alluxio.worker.tieredstore.levelX"
          + ".watermark.{high/low}.ratio instead");
      put(Name.WORKER_TIERED_STORE_RETRY, V2_0_0_REMOVED);

      put(Name.TEST_REMOVED_KEY, "This key is used only for testing. It is always removed");
    }
  };

  static final class Name {
    public static final String MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL =
        "alluxio.master.client.socket.cleanup.interval";
    public static final String MASTER_CONNECTION_TIMEOUT =
        "alluxio.master.connection.timeout";
    public static final String MASTER_FILE_ASYNC_PERSIST_HANDLER =
        "alluxio.master.file.async.persist.handler";
    public static final String MASTER_HEARTBEAT_INTERVAL =
        "alluxio.master.heartbeat.interval";
    public static final String MASTER_JOURNAL_FORMATTER_CLASS =
        "alluxio.master.journal.formatter.class";
    public static final String MASTER_RETRY = "alluxio.master.retry";
    public static final String SWIFT_API_KEY = "fs.swift.apikey";
    public static final String SWIFT_USE_PUBLIC_URI_KEY = "fs.swift.use.public.url";
    public static final String UNDERFS_S3A_CONSISTENCY_TIMEOUT_MS =
        "alluxio.underfs.s3a.consistency.timeout";
    public static final String USER_BLOCK_WORKER_CLIENT_THREADS =
        "alluxio.user.block.worker.client.threads";
    public static final String USER_FAILED_SPACE_REQUEST_LIMITS =
        "alluxio.user.failed.space.request.limits";
    public static final String USER_FILE_CACHE_PARTIALLY_READ_BLOCK =
        "alluxio.user.file.cache.partially.read.block";
    public static final String USER_FILE_SEEK_BUFFER_SIZE_BYTES =
        "alluxio.user.file.seek.buffer.size.bytes";
    public static final String USER_NETWORK_SOCKET_TIMEOUT =
        "alluxio.user.network.socket.timeout";
    public static final String USER_RPC_RETRY_MAX_NUM_RETRY =
        "alluxio.user.rpc.retry.max.num.retry";
    public static final String USER_UFS_DELEGATION_READ_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.read.buffer.size.bytes";
    public static final String USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES =
        "alluxio.user.ufs.delegation.write.buffer.size.bytes";
    public static final String USER_HEARTBEAT_INTERVAL = "alluxio.user.heartbeat.interval";
    public static final String USER_HOSTNAME = "alluxio.user.hostname";
    public static final String WEB_TEMP_PATH = "alluxio.web.temp.path";
    public static final String WORKER_BLOCK_THREADS_MAX = "alluxio.worker.block.threads.max";
    public static final String WORKER_BLOCK_THREADS_MIN = "alluxio.worker.block.threads.min";
    public static final String WORKER_DATA_BIND_HOST = "alluxio.worker.data.bind.host";
    public static final String WORKER_DATA_PORT = "alluxio.worker.data.port";
    public static final String WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO =
        Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(0);
    public static final String WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO =
        Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(1);
    public static final String WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO =
        Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(2);
    public static final String WORKER_TIERED_STORE_RETRY = "alluxio.worker.tieredstore.retry";

    public static final String TEST_REMOVED_KEY = "alluxio.test.removed.key";
  }

  static final class Template {

    private static final List<Template> TEMPLATES = new ArrayList<>();

    public static final Template WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO = new Template(
        "alluxio.worker.tieredstore.level%d.reserved.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.reserved\\.ratio",
        "The keys associated with this template have been removed");

    private final String mFormat;
    private final Pattern mPattern;
    private final String mMessage;

    private Template(String format, String re, String removalMessage) {
      mFormat = format;
      mPattern = Pattern.compile(re);
      mMessage = removalMessage;
      TEMPLATES.add(this);
    }

    private String format(Object... o) {
      return String.format(mFormat, o);
    }

    private boolean matches(String input) {
      Matcher matcher = mPattern.matcher(input);
      return matcher.matches();
    }
  }

  /**
   * returns whether or not the given property key exists in the removed key list.
   *
   * @param key the property key to check
   * @return whether or not the key has been removed
   */
  static boolean isRemoved(String key) {
    return getMessage(key) != null;
  }

  /**
   * gets the message pertaining to a removed key or template.
   *
   * @param key the property key to check
   * @return whether or not the key has been removed
   */
  @Nullable
  public static String getMessage(String key) {
    String msg;
    if ((msg = REMOVED_KEYS.getOrDefault(key, null)) != null) {
      return msg;
    }

    for (Template t : Template.TEMPLATES) {
      if (t.matches(key)) {
        return t.mMessage;
      }
    }
    return null;
  }
}
