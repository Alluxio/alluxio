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

package alluxio.util;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalType;

/**
 * Utilities to detect features that Alluxio is running with.
 */
public final class FeatureUtils {
  /**
   * Utility to check the master journal type is embedded journal.
   *
   * @return true, if running with embedded journal
   */
  public static boolean isEmbeddedJournal() {
    return JournalType.EMBEDDED.toString()
        .equals(ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_TYPE));
  }

  /**
   * Utility to check the master metastore is rocks.
   *
   * @return true, if running with rocks
   */
  public static boolean isRocks() {
    return "ROCKS".equals(ServerConfiguration.get(PropertyKey.MASTER_METASTORE));
  }

  /**
   * Utility to check Zookeeper is enabled.
   *
   * @return true, if Zookeeper is enabled
   */
  public static boolean isZookeeperEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  /**
   * Utility to check back delegation is enabled.
   *
   * @return true, if backup delegation is enabled
   */
  public static boolean isBackupDelegationEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED);
  }

  /**
   * Utility to check daily backup is enabled.
   *
   * @return true, if daily backup is enabled
   */
  public static boolean isDailyBackupEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.MASTER_DAILY_BACKUP_ENABLED);
  }

  /**
   * Utility to check persistence black list is empty.
   *
   * @return true, if persistence black list is empty
   */
  public static boolean isPersistenceBlacklistEmpty() {
    return !ServerConfiguration.isSet(PropertyKey.MASTER_PERSISTENCE_BLACKLIST)
        || ServerConfiguration.get(PropertyKey.MASTER_PERSISTENCE_BLACKLIST).isEmpty();
  }

  /**
   * Utility to check unsafe direct persistence is enabled.
   *
   * @return true, if unsafe direct persistence is enabled
   */
  public static boolean isUnsafeDirectPersistEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED);
  }

  /**
   * Utility to check master audit logging is enabled.
   *
   * @return true, if master audir logging is enabled
   */
  public static boolean isMasterAuditLoggingEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED);
  }
}
