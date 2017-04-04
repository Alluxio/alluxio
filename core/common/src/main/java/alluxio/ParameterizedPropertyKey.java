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

import alluxio.exception.ExceptionMessage;

import com.google.common.base.Objects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of parameterized property keys that can generate the property name given parameters.
 * E.g., * {@code ParameterizedPropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
 */
@ThreadSafe
public enum ParameterizedPropertyKey {
  MASTER_MOUNT_TABLE_ENTRY_ALLUXIO("alluxio.master.mount.table.%s.alluxio",
      "alluxio.master.mount.table.(\\w+).alluxio"),
  MASTER_MOUNT_TABLE_ENTRY_OPTION("alluxio.master.mount.table.%s.option",
      "alluxio.master.mount.table.(\\w+).option"),
  MASTER_MOUNT_TABLE_ENTRY_READONLY("alluxio.master.mount.table.%s.readonly",
      "alluxio.master.mount.table.(\\w+).readonly"),
  MASTER_MOUNT_TABLE_ENTRY_SHARED("alluxio.master.mount.table.%s.shared",
      "alluxio.master.mount.table.(\\w+).shared"),
  MASTER_MOUNT_TABLE_ENTRY_UFS("alluxio.master.mount.table.%s.ufs",
      "alluxio.master.mount.table.(\\w+).ufs"),
  MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS("alluxio.master.tieredstore.global.level%d.alias",
      "alluxio.master.tieredstore.global.level(\\d+).alias"),
  WORKER_TIERED_STORE_LEVEL_ALIAS("alluxio.worker.tieredstore.level%d.alias",
      "alluxio.worker.tieredstore.level(\\d+).alias"),
  WORKER_TIERED_STORE_LEVEL_DIRS_PATH("alluxio.worker.tieredstore.level%d.dirs.path",
      "alluxio.worker.tieredstore.level(\\d+).dirs.path"),
  WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA("alluxio.worker.tieredstore.level%d.dirs.quota",
      "alluxio.worker.tieredstore.level(\\d+).dirs.quota"),
  WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO("alluxio.worker.tieredstore.level%d.reserved.ratio",
      "alluxio.worker.tieredstore.level(\\d+).reserved.ratio"),;

  private final String mFormat;
  private final Pattern mPattern;

  /**
   * @param input string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String input) {
    for (ParameterizedPropertyKey key : values()) {
      Matcher matcher = key.mPattern.matcher(input);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param input string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String input) {
    for (ParameterizedPropertyKey key : values()) {
      Matcher matcher = key.mPattern.matcher(input);
      if (matcher.matches()) {
        return new PropertyKey(input);
      }
    }
    throw new IllegalArgumentException(
        ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(input));
  }

  /**
   * Constructs a property key format.
   *
   * @param format String of this format
   */
  ParameterizedPropertyKey(String format, String re) {
    mFormat = format;
    mPattern = Pattern.compile(re);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("format", mFormat).add("pattern", mPattern).toString();
  }

  /**
   * Converts a property key template (e.g.,
   * {@link ParameterizedPropertyKey#WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO})
   * to a {@link PropertyKey} instance.
   *
   * @param params ordinal
   * @return corresponding property
   */
  public PropertyKey format(Object... params) {
    return new PropertyKey(String.format(mFormat, params));
  }
}
