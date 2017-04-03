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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of parameterized property keys that can generate the property name given parameters.
 * E.g., * {@code ParameterizedPropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
 *
 */
@ThreadSafe
public enum ParameterizedPropertyKey {
  MASTER_MOUNT_TABLE_ENTRY_ALLUXIO("alluxio.master.mount.table.%s.alluxio"),
  MASTER_MOUNT_TABLE_ENTRY_OPTION("alluxio.master.mount.table.%s.option"),
  MASTER_MOUNT_TABLE_ENTRY_READONLY("alluxio.master.mount.table.%s.readonly"),
  MASTER_MOUNT_TABLE_ENTRY_SHARED("alluxio.master.mount.table.%s.shared"),
  MASTER_MOUNT_TABLE_ENTRY_UFS("alluxio.master.mount.table.%s.ufs"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS("alluxio.master.tieredstore.global.level%d.alias"),
  WORKER_TIERED_STORE_LEVEL_ALIAS("alluxio.worker.tieredstore.level%d.alias"),
  WORKER_TIERED_STORE_LEVEL_DIRS_PATH("alluxio.worker.tieredstore.level%d.dirs.path"),
  WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA("alluxio.worker.tieredstore.level%d.dirs.quota"),
  WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO("alluxio.worker.tieredstore.level%d.reserved.ratio"),
  ;

  private final String mFormat;

  /**
   * Constructs a property key format.
   *
   * @param format String of this format
   */
  ParameterizedPropertyKey(String format) {
    mFormat = format;
  }

  @Override
  public String toString() {
    return mFormat;
  }

  /**
   * Converts a property key template (e.g.,
   * {@link ParameterizedPropertyKey#WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO})
   * to a {@link PropertyKey} enum instance.
   *
   * @param params ordinal
   * @return corresponding property
   */
  public PropertyKey format(Object... params) {
    return new PropertyKey(String.format(mFormat, params));
  }
}
