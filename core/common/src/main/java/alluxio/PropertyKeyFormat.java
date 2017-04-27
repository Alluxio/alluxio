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
 * A set of format templates to generate the property name for a given level in tiered storage. e
 * .g.,
 * {@code PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(0)}
 */
@ThreadSafe
public enum PropertyKeyFormat {
  MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT("alluxio.master.tieredstore.global.level%d.alias"),
  WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT("alluxio.worker.tieredstore.level%d.alias"),
  WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT("alluxio.worker.tieredstore.level%d.dirs.path"),
  WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT("alluxio.worker.tieredstore.level%d.dirs.quota"),
  WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT(
      "alluxio.worker.tieredstore.level%d.watermark.high.ratio"),
  WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO_FORMAT(
      "alluxio.worker.tieredstore.level%d.watermark.low.ratio"),
  /**
   * @deprecated It will be removed in 2.0.0.
   * Use {@link #WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT} and
   * {@link #WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO_FORMAT} instead.
   */
  @Deprecated
  WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT(
      "alluxio.worker.tieredstore.level%d.reserved.ratio"),
  ;

  private final String mFormat;

  /**
   * Constructs a property key format.
   *
   * @param format String of this format
   */
  PropertyKeyFormat(String format) {
    mFormat = format;
  }

  @Override
  public String toString() {
    return mFormat;
  }

  /**
   * Converts a property key template (e.g.,
   * {@link PropertyKeyFormat#WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT})
   * to a {@link PropertyKey} enum instance.
   *
   * @param params ordinal
   * @return corresponding property
   */
  public PropertyKey format(Object... params) {
    return PropertyKey.fromString(String.format(mFormat, params));
  }
}
