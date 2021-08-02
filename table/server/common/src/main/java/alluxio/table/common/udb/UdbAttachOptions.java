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

package alluxio.table.common.udb;

import alluxio.collections.Pair;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbAttachOptions {
  private static final Logger LOG = LoggerFactory.getLogger(UdbAttachOptions.class);

  /**
   * Bypass/ignore entries.
   */
  public enum Entry {
    BYPASS, IGNORE, NONE;
  }

  /**
   * Inclusion/exclusion modes of tables and partitions.
   */
  public enum Mode {
    INCLUDE, EXCLUDE, NONE;
  }

  /**
   * Bypassed tables.
   * When this set is empty, mTableBypassMode is set to NONE.
   */
  private final Set<Name> mBypassedTables;
  /**
   * Mode of bypassed tables: included or excluded.
   */
  private final Mode mTableBypassMode;
  /**
   * Map of table names to their bypassed partitions and mode.
   */
  private final Map<String, Pair<Mode, Set<Name>>> mBypassedPartitions;
  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   * When this set is empty, mIgnoreMode is set to NONE.
   */
  private final Set<Name> mIgnoredTables;
  /**
   * Mode of ignored tables: included or excluded.
   */
  private final Mode mIgnoreMode;

  private UdbAttachOptions(
      Set<Name> bypassedTables,
      Mode tableBypassMode,
      Map<String, Pair<Mode, Set<Name>>> bypassedPartitions,
      Set<Name> ignoredTables,
      Mode ignoreMode) {
    mBypassedTables = bypassedTables;
    mIgnoredTables = ignoredTables;
    mBypassedPartitions = bypassedPartitions;
    mTableBypassMode = tableBypassMode;
    mIgnoreMode = ignoreMode;
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbAttachOptions#isFullyBypassedTable(String)
   */
  public boolean isBypassedTable(String tableName) {
    boolean isSpecifiedByName = isContainedInWrappers(mBypassedTables, tableName);
    if (isSpecifiedByName) {
      if (mTableBypassMode == Mode.EXCLUDE) {
        return false;
      }
      return shadowedByIgnore(tableName, true);
    }
    boolean hasBypassedPartition = mBypassedPartitions.containsKey(tableName);
    if (hasBypassedPartition) {
      // no need to check mTableBypassMode here, bc if a table has partition specifications,
      // it must be in inclusion mode (or none)
      return shadowedByIgnore(tableName, true);
    }
    // the table is not listed in any way
    if (mTableBypassMode == Mode.EXCLUDE) {
      return shadowedByIgnore(tableName, true);
    } else {
      return false;
    }
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbAttachOptions#isBypassedTable(String)
   */
  public boolean isFullyBypassedTable(String tableName) {
    boolean isFullyBypassed = isContainedInWrappers(mBypassedTables, tableName);
    return shadowedByIgnore(tableName, handleInclusionExclusion(mTableBypassMode, isFullyBypassed));
  }

  /**
   * Checks if a table is ignored.
   *
   * @param tableName the table name
   * @return true if the table is ignored, false otherwise
   */
  public boolean isIgnoredTable(String tableName) {
    boolean isIgnored = isContainedInWrappers(mIgnoredTables, tableName);
    return handleInclusionExclusion(mIgnoreMode, isIgnored);
  }

  /**
   * Checks by a partition's name if it should be bypassed.
   *
   * @param tableName the table name
   * @param partitionName the partition name
   * @return true if the partition should be bypassed, false otherwise
   */
  public boolean isBypassedPartition(String tableName, String partitionName) {
    if (isFullyBypassedTable(tableName)) {
      return true;
    }
    if (!mBypassedPartitions.containsKey(tableName)) {
      return false;
    }
    Mode partitionMode = mBypassedPartitions.get(tableName).getFirst();
    boolean isBypassed =
        isContainedInWrappers(mBypassedPartitions.get(tableName).getSecond(), partitionName);
    return shadowedByIgnore(tableName, handleInclusionExclusion(partitionMode, isBypassed));
  }

  private static boolean handleInclusionExclusion(Mode mode, boolean selected) {
    switch (mode) {
      case INCLUDE:
        return selected;
      case EXCLUDE:
        return !selected;
      case NONE:
      default:
        return false;
    }
  }

  /**
   * Checks if a table is shadowed by ignoring when it is also bypassed.
   * @param tableName the table name
   * @param mightBeShadowedIfTrue the return value of {@link #isBypassedTable(String)}, etc
   * @return false if the table is shadowed by ignoring, mightBeShadowedIfTrue if it is not
   */
  private boolean shadowedByIgnore(String tableName, boolean mightBeShadowedIfTrue) {
    if (mightBeShadowedIfTrue && isIgnoredTable(tableName)) {
      LOG.warn("Table {} is set to be bypassed but it is also ignored", tableName);
      return false;
    }
    return mightBeShadowedIfTrue;
  }

  private static boolean isContainedInWrappers(Set<Name> wrappers, String name) {
    return wrappers.stream().anyMatch(p -> p.matches(name));
  }

  private static <T> T replaceNullWith(T nullable, T defaultOnNull) {
    if (nullable == null) {
      return defaultOnNull;
    }
    return nullable;
  }

  private static <K, V> Map<K, V>  replaceNullWithEmpty(Map<K, V> someMap) {
    return replaceNullWith(someMap, Collections.emptyMap());
  }

  private static <K> Set<K> replaceNullWithEmpty(Set<K> someSet) {
    return replaceNullWith(someSet, Collections.emptySet());
  }

  /**
   * Abstract name for tables and partitions.
   */
  interface Name {
    /**
     * Checks whether the argument name matches the configured name pattern.
     *
     * @param name the concrete name to check for
     * @return true if the names match, false otherwise
     */
    boolean matches(String name);
  }

  static class ExactName implements Name {
    private final String mExactName;

    public ExactName(String exactName) {
      Preconditions.checkArgument(!exactName.isEmpty(), "empty name");
      mExactName = exactName;
    }

    @Override
    public boolean matches(String name) {
      return mExactName.equals(name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExactName exactName = (ExactName) o;
      return Objects.equals(mExactName, exactName.mExactName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mExactName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("exactName", mExactName)
          .toString();
    }
  }

  static class PatternName implements Name {
    private final Pattern mPattern;

    public PatternName(Pattern pattern) {
      mPattern = pattern;
    }

    @Override
    public boolean matches(String name) {
      return mPattern.matcher(name).matches();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PatternName that = (PatternName) o;
      return Objects.equals(mPattern, that.mPattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mPattern);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("pattern", mPattern)
          .toString();
    }
  }

  /**
   * Builder for {@link UdbAttachOptions}.
   */
  public static class Builder {
    private final Set<Name> mBypassedTables;
    private final Set<Name> mIgnoredTables;
    private final Map<String, Set<Name>> mBypassedPartitions;
    private final Map<String, Mode> mPartitionBypassModes;
    private Mode mTableBypassMode;
    private Mode mIgnoreMode;

    /**
     * Creates a new builder.
     */
    public Builder() {
      mTableBypassMode = Mode.NONE;
      mIgnoreMode = Mode.NONE;
      mBypassedTables = new HashSet<>();
      mIgnoredTables = new HashSet<>();
      mBypassedPartitions = new HashMap<>();
      mPartitionBypassModes = new HashMap<>();
    }

    /**
     * Include or exclude the bypassed tables.
     * @param mode {@link Mode} of the tables
     * @return this builder
     */
    public Builder setBypassedTablesMode(Mode mode) {
      mTableBypassMode = mode;
      return this;
    }

    /**
     * Include or exclude the ignored tables.
     * @param mode {@link Mode} of the tables
     * @return this builder
     */
    public Builder setIgnoredTablesMode(Mode mode) {
      mIgnoreMode = mode;
      return this;
    }

    /**
     * Add a table by its exact name to the bypass entry.
     * @param name table name
     * @return this builder
     */
    public Builder addBypassedTable(String name) {
      return addTable(new ExactName(name), Entry.BYPASS);
    }

    /**
     * Add a table by pattern to the bypass entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addBypassedTable(Pattern pattern) {
      return addTable(new PatternName(pattern), Entry.BYPASS);
    }

    /**
     * Add a table by its exact name to the ignore entry.
     * @param name table name
     * @return this builder
     */
    public Builder addIgnoredTable(String name) {
      return addTable(new ExactName(name), Entry.IGNORE);
    }

    /**
     * Add a table by pattern to the ignore entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addIgnoredTable(Pattern pattern) {
      return addTable(new PatternName(pattern), Entry.IGNORE);
    }

    private Builder addTable(Name item, Entry entry) {
      switch (entry) {
        case BYPASS:
          mBypassedTables.add(item);
          break;
        case IGNORE:
          mIgnoredTables.add(item);
          break;
        case NONE:
        default:
          // unreachable
      }
      return this;
    }

    /**
     * Include or exclude the partitions of a bypassed tabls.
     * @param tableName name of the parent table
     * @param mode {@link Mode} of the tables
     * @return this builder
     */
    public Builder setBypassedPartitionsMode(String tableName, Mode mode) {
      mPartitionBypassModes.put(tableName, mode);
      return this;
    }

    /**
     * Add a partition by its exact name.
     * @param tableName the containing table
     * @param partitionName partition name
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, String partitionName) {
      return addBypassedPartition(tableName, new ExactName(partitionName));
    }

    /**
     * Add a partition by pattern.
     * @param tableName the containing table
     * @param partitionPattern partition pattern
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, Pattern partitionPattern) {
      return addBypassedPartition(tableName, new PatternName(partitionPattern));
    }

    private Builder addBypassedPartition(String tableName, Name partition) {
      mBypassedPartitions.compute(tableName, (tblName, set) -> {
        if (set == null) {
          return Sets.newHashSet(partition);
        } else {
          set.add(partition);
          return set;
        }
      });
      return this;
    }

    /**
     * Build a {@link UdbAttachOptions}.
     * @return {@link UdbAttachOptions}
     */
    public UdbAttachOptions build() {
      if (!mBypassedTables.isEmpty()) {
        Preconditions.checkState(mTableBypassMode != null && mTableBypassMode != Mode.NONE,
            "Missing inclusion/exclusion mode for bypassed tables");
      } else {
        // override mode to NONE when no tables are given
        mTableBypassMode = Mode.NONE;
      }
      if (!mIgnoredTables.isEmpty()) {
        Preconditions.checkState(mIgnoreMode != null && mIgnoreMode != Mode.NONE,
            "Missing inclusion/exclusion mode for ignored tables");
      } else {
        mIgnoreMode = Mode.NONE;
      }
      Map<String, Pair<Mode, Set<Name>>> partitions = new HashMap<>();
      if (!mBypassedPartitions.isEmpty()) {
        // forbid adding partitions when tables are excluded
        Preconditions.checkState(mTableBypassMode != Mode.EXCLUDE,
            "Tables are specified with exclusion mode, cannot have partition specification");
        for (Map.Entry<String, Set<Name>> entry : mBypassedPartitions.entrySet()) {
          String tableName = entry.getKey();
          Set<Name> partitionNames = entry.getValue();
          // make sure this table has associated partition mode
          Preconditions.checkState(mPartitionBypassModes.containsKey(tableName)
              && mPartitionBypassModes.get(tableName) != Mode.NONE,
              "Missing partitions mode for table {}",
              tableName);
          // forbid both exact name and partition spec for a table at the same time
          Preconditions.checkState(mBypassedTables.stream()
                  .noneMatch(name -> name instanceof ExactName && name.matches(tableName)),
              "Table {} is already specified with exact name, "
                  + "cannot have partition specification at the same time",
              tableName);
          Preconditions.checkState(!partitionNames.isEmpty(),
              "Empty partition specification set for table {}",
              tableName);
          partitions.put(tableName,
              new Pair<>(mPartitionBypassModes.get(tableName), partitionNames));
        }
      }
      return new UdbAttachOptions(
          mBypassedTables,
          mTableBypassMode,
          partitions,
          mIgnoredTables,
          mIgnoreMode
      );
    }
  }
}
