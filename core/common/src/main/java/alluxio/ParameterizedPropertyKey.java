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

import com.google.common.base.Objects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of parameterized property keys that can generate the property name given parameters.
 * E.g., * {@code Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
 */
@ThreadSafe
public final class ParameterizedPropertyKey extends PropertyKey {
  /**
   * A set of templates to generate the names of parameterized properties given
   * different parameters. E.g., * {@code Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)}
   */
  @ThreadSafe
  public enum Template {
    MASTER_MOUNT_TABLE_ENTRY_ALLUXIO("alluxio.master.mount.table.%s.alluxio",
        "alluxio\\.master\\.mount\\.table.(\\w+)\\.alluxio"),
    MASTER_MOUNT_TABLE_ENTRY_OPTION("alluxio.master.mount.table.%s.option",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option"),
    MASTER_MOUNT_TABLE_ENTRY_OPTION_PROPERTY("alluxio.master.mount.table.%s.option.%s",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.option(\\.\\w+)++"),
    MASTER_MOUNT_TABLE_ENTRY_READONLY("alluxio.master.mount.table.%s.readonly",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.readonly"),
    MASTER_MOUNT_TABLE_ENTRY_SHARED("alluxio.master.mount.table.%s.shared",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.shared"),
    MASTER_MOUNT_TABLE_ENTRY_UFS("alluxio.master.mount.table.%s.ufs",
        "alluxio\\.master\\.mount\\.table\\.(\\w+)\\.ufs"),
    MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS("alluxio.master.tieredstore.global.level%d.alias",
        "alluxio\\.master\\.tieredstore\\.global\\.level(\\d+)\\.alias"),
    WORKER_TIERED_STORE_LEVEL_ALIAS("alluxio.worker.tieredstore.level%d.alias",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.alias"),
    WORKER_TIERED_STORE_LEVEL_DIRS_PATH("alluxio.worker.tieredstore.level%d.dirs.path",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.path"),
    WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA("alluxio.worker.tieredstore.level%d.dirs.quota",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.dirs\\.quota"),
    WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO("alluxio.worker.tieredstore.level%d.reserved.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.reserved\\.ratio"),
    ;

    private final String mFormat;
    private final Pattern mPattern;

    /**
     * Constructs a property key format.
     *
     * @param format String of this property as formatted string
     * @param re String of this property as regexp
     */
    Template(String format, String re) {
      mFormat = format;
      mPattern = Pattern.compile(re);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("format", mFormat).add("pattern", mPattern)
          .toString();
    }

    /**
     * Converts a property key template (e.g.,
     * {@link #WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO}) to a {@link PropertyKey} instance.
     *
     * @param params ordinal
     * @return corresponding property
     */
    public ParameterizedPropertyKey format(Object... params) {
      return new ParameterizedPropertyKey(String.format(mFormat, params));
    }
  }

  /**
   * @param input string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String input) {
    for (Template template : Template.values()) {
      Matcher matcher = template.mPattern.matcher(input);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a string and returns its corresponding {@link PropertyKey}, throwing exception if no
   * such a property can be found.
   *
   * @param input string of property key
   * @return corresponding property
   */
  public static ParameterizedPropertyKey fromString(String input) {
    for (Template template : Template.values()) {
      Matcher matcher = template.mPattern.matcher(input);
      if (matcher.matches()) {
        return new ParameterizedPropertyKey(input);
      }
    }
    return null;
  }

  /**
   * @param name String of this property name
   */
  private ParameterizedPropertyKey(String name) {
    super(name);
  }
}
