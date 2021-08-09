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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
 * Tables and partitions filtering specification.
 */
public final class UdbFilterSpec {
  private static final Logger LOG = LoggerFactory.getLogger(UdbFilterSpec.class);
  private static final UdbFilterSpec EMPTY_INSTANCE =
      new UdbFilterSpec(Collections.emptySet(),
          EntryMode.NONE,
          Collections.emptyMap(),
          Collections.emptySet(),
          EntryMode.NONE);

  /**
   * Type of entries.
   */
  public enum EntryType {
    /**
     * Bypassed entries.
     *
     * Tables and partitions specified in these entries are not mounted to Alluxio's file system,
     * but are visible to SDS clients. Clients are instructed to access their data directly
     * from the UFS.
     *
     */
    BYPASS,
    /**
     * Ignored entries.
     *
     * Tables specified in these entries are not visible to SDS clients, nor can they be accessed
     * through Alluxio's file system.
     */
    IGNORE
  }

  /**
   * Inclusion/exclusion modes of entries.
   */
  public enum EntryMode {
    /**
     * Inclusion mode.
     *
     * This mode indicates that the configuration should be applied to the items specified
     * by the entry.
     */
    INCLUDE,
    /**
     * Exclusion mode.
     *
     * This mode indicates that the configuration should be applied to any item other than
     * the items specified by the entry.
     * An empty exclusion entry does not cause the configuration to be applied to all items,
     * instead it is the same as an empty inclusion entry.
     */
    EXCLUDE,
    /**
     * The none mode.
     *
     * A placeholder mode that is invalid for any real configuration.
     */
    NONE
  }

  /**
   * Bypassed tables.
   * When this set is empty, {@link #mTableBypassMode} is set to {@link EntryMode#NONE}.
   */
  private final Set<EntryName> mBypassedTables;
  /**
   * Mode of bypassed tables.
   * {@link EntryMode#NONE} if {@link #mBypassedTables} is empty.
   */
  private final EntryMode mTableBypassMode;
  /**
   * Map of table names to their bypassed partitions and mode.
   */
  private final Map<String, Pair<EntryMode, Set<EntryName>>> mBypassedPartitions;
  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   * When this set is empty, mIgnoreMode is set to {@link EntryMode#NONE}.
   */
  private final Set<EntryName> mIgnoredTables;
  /**
   * Mode of ignored tables.
   * {@link EntryMode#NONE} if {@link #mIgnoredTables} is empty.
   */
  private final EntryMode mIgnoreMode;

  private UdbFilterSpec(
      Set<EntryName> bypassedTables,
      EntryMode tableBypassMode,
      Map<String, Pair<EntryMode, Set<EntryName>>> bypassedPartitions,
      Set<EntryName> ignoredTables,
      EntryMode ignoreMode) {
    mBypassedTables = ImmutableSet.copyOf(bypassedTables);
    mIgnoredTables = ImmutableSet.copyOf(ignoredTables);
    mBypassedPartitions = ImmutableMap.copyOf(bypassedPartitions);

    if (!mBypassedTables.isEmpty()) {
      Preconditions.checkArgument(tableBypassMode != null && tableBypassMode != EntryMode.NONE,
          "Missing inclusion/exclusion mode for bypassed tables");
      mTableBypassMode = tableBypassMode;
    } else {
      // override mode to NONE when no tables are given
      mTableBypassMode = EntryMode.NONE;
    }
    if (!mIgnoredTables.isEmpty()) {
      Preconditions.checkArgument(ignoreMode != null && ignoreMode != EntryMode.NONE,
          "Missing inclusion/exclusion mode for ignored tables");
      mIgnoreMode = ignoreMode;
    } else {
      mIgnoreMode = EntryMode.NONE;
    }
    if (!mBypassedPartitions.isEmpty()) {
      // forbid having partition spec when tables are excluded
      Preconditions.checkArgument(mTableBypassMode != EntryMode.EXCLUDE,
          "Tables are specified with exclusion mode, cannot have partition specification");
      for (Map.Entry<String, Pair<EntryMode, Set<EntryName>>> entry :
          mBypassedPartitions.entrySet()) {
        String tableName = entry.getKey();
        EntryMode partitionMode = entry.getValue().getFirst();
        Set<EntryName> partitionEntryNames = entry.getValue().getSecond();
        // forbid empty table name
        Preconditions.checkArgument(!tableName.isEmpty(),
            "Empty table name specified with partition specification %s",
            partitionEntryNames);
        // forbid empty spec
        Preconditions.checkArgument(!partitionEntryNames.isEmpty(),
            "Empty partition specification set for table %s",
            tableName);
        // forbid NONE mode
        Preconditions.checkArgument(partitionMode != EntryMode.NONE,
            "Invalid mode %s for partitions of table %s, must be either include or exclude",
            partitionMode, tableName);
        // forbid both exact name and partition spec for a table at the same time
        Preconditions.checkArgument(mBypassedTables.stream()
                .noneMatch(template -> template instanceof ExactEntryName
                    && template.matches(tableName)),
            "Table %s is already specified with exact name, "
                + "cannot have partition specification at the same time",
            tableName);
      }
    }
  }

  /**
   * Returns an empty instance.
   * @return empty instance
   */
  public static UdbFilterSpec empty() {
    return EMPTY_INSTANCE;
  }

  /**
   * Checks if a table is bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbFilterSpec#isFullyBypassedTable(String)
   */
  public boolean isBypassedTable(String tableName) {
    boolean isSpecifiedByName = matchedByTemplates(mBypassedTables, tableName);
    if (isSpecifiedByName) {
      if (mTableBypassMode == EntryMode.EXCLUDE) {
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
    if (mTableBypassMode == EntryMode.EXCLUDE) {
      return shadowedByIgnore(tableName, true);
    } else {
      return false;
    }
  }

  /**
   * Checks if a table is fully bypassed.
   * A table is fully bypassed, if all its partitions are bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbFilterSpec#isBypassedTable(String)
   */
  public boolean isFullyBypassedTable(String tableName) {
    boolean isFullyBypassed = matchedByTemplates(mBypassedTables, tableName);
    return shadowedByIgnore(tableName, handleInclusionExclusion(mTableBypassMode, isFullyBypassed));
  }

  /**
   * Checks if a table is ignored.
   *
   * @param tableName the table name
   * @return true if the table is ignored, false otherwise
   */
  public boolean isIgnoredTable(String tableName) {
    boolean isIgnored = matchedByTemplates(mIgnoredTables, tableName);
    return handleInclusionExclusion(mIgnoreMode, isIgnored);
  }

  /**
   * Checks if a partition of a table is bypassed.
   *
   * @param tableName the name of the table where the partition belongs
   * @param partitionName the partition name
   * @return true if the partition is bypassed, false otherwise
   */
  public boolean isBypassedPartition(String tableName, String partitionName) {
    if (isFullyBypassedTable(tableName)) {
      return true;
    }
    if (!mBypassedPartitions.containsKey(tableName)) {
      return false;
    }
    EntryMode partitionMode = mBypassedPartitions.get(tableName).getFirst();
    boolean isBypassed =
        matchedByTemplates(mBypassedPartitions.get(tableName).getSecond(), partitionName);
    return shadowedByIgnore(tableName, handleInclusionExclusion(partitionMode, isBypassed));
  }

  /**
   * Handles inclusion and exclusion logic.
   *
   * Inverts {@code selected} on {@link EntryMode#EXCLUDE} mode, does nothing on
   * {@link EntryMode#INCLUDE} mode.
   * @param entryMode the mode of the entry
   * @param selected whether an item is specified by an entry
   * @return {@code selected} on {@link EntryMode#INCLUDE} mode,
   *         {@code !selected} on {@link EntryMode#EXCLUDE} mode
   */
  private static boolean handleInclusionExclusion(EntryMode entryMode, boolean selected) {
    switch (entryMode) {
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
      // todo(bowen): this warning is never triggered because if a table is ignored,
      // it will not be checked for bypassing, given the current implementation in Database
      LOG.warn("Table {} is set to be bypassed but it is also ignored", tableName);
      return false;
    }
    return mightBeShadowedIfTrue;
  }

  private static boolean matchedByTemplates(Set<EntryName> templates, String name) {
    return templates.stream().anyMatch(p -> p.matches(name));
  }

  /**
   * Abstract name for tables and partitions.
   */
  interface EntryName {
    /**
     * Checks whether the argument name matches the configured name pattern.
     *
     * @param name the concrete name to check for
     * @return true if the names match, false otherwise
     */
    boolean matches(String name);
  }

  static class ExactEntryName implements EntryName {
    private final String mExactName;

    public ExactEntryName(String exactName) {
      Preconditions.checkNotNull(exactName, "exactName");
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
      ExactEntryName that = (ExactEntryName) o;
      return Objects.equals(mExactName, that.mExactName);
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

  static class PatternEntryName implements EntryName {
    private final Pattern mPattern;

    public PatternEntryName(Pattern pattern) {
      mPattern = Preconditions.checkNotNull(pattern, "pattern");
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
      PatternEntryName that = (PatternEntryName) o;
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
   * Builder for {@link UdbFilterSpec}.
   */
  public static class Builder {
    private final Set<EntryName> mBypassedTables;
    private final Set<EntryName> mIgnoredTables;
    private final Map<String, Set<EntryName>> mBypassedPartitions;
    private final Map<String, EntryMode> mPartitionBypassModes;
    private EntryMode mTableBypassMode;
    private EntryMode mIgnoreMode;

    /**
     * Creates a new builder.
     */
    public Builder() {
      mTableBypassMode = EntryMode.NONE;
      mIgnoreMode = EntryMode.NONE;
      mBypassedTables = new HashSet<>();
      mIgnoredTables = new HashSet<>();
      mBypassedPartitions = new HashMap<>();
      mPartitionBypassModes = new HashMap<>();
    }

    /**
     * Sets the mode of bypassed tables.
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setBypassedTablesMode(EntryMode entryMode) {
      mTableBypassMode = entryMode;
      return this;
    }

    /**
     * Sets the mode of ignored tables.
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setIgnoredTablesMode(EntryMode entryMode) {
      mIgnoreMode = entryMode;
      return this;
    }

    /**
     * Adds a table by its exact name to the bypass entry.
     * @param name table name
     * @return this builder
     */
    public Builder addBypassedTable(String name) {
      return addTable(new ExactEntryName(name), EntryType.BYPASS);
    }

    /**
     * Adds a table by pattern to the bypass entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addBypassedTable(Pattern pattern) {
      return addTable(new PatternEntryName(pattern), EntryType.BYPASS);
    }

    /**
     * Adds a table by its exact name to the ignore entry.
     * @param name table name
     * @return this builder
     */
    public Builder addIgnoredTable(String name) {
      return addTable(new ExactEntryName(name), EntryType.IGNORE);
    }

    /**
     * Adds a table by pattern to the ignore entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addIgnoredTable(Pattern pattern) {
      return addTable(new PatternEntryName(pattern), EntryType.IGNORE);
    }

    private Builder addTable(EntryName item, EntryType entryType) {
      switch (entryType) {
        case BYPASS:
          mBypassedTables.add(item);
          break;
        case IGNORE:
          mIgnoredTables.add(item);
          break;
        default:
          throw new IllegalStateException(String.format("Invalid entry type: %s", entryType));
      }
      return this;
    }

    /**
     * Sets the mode of the partitions of a bypassed table.
     * @param tableName name of the parent table
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setBypassedPartitionsMode(String tableName, EntryMode entryMode) {
      mPartitionBypassModes.put(tableName, entryMode);
      return this;
    }

    /**
     * Adds a partition by its exact name.
     * @param tableName the containing table
     * @param partitionName partition name
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, String partitionName) {
      return addBypassedPartition(tableName, new ExactEntryName(partitionName));
    }

    /**
     * Adds a partition by pattern.
     * @param tableName the containing table
     * @param partitionPattern partition pattern
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, Pattern partitionPattern) {
      return addBypassedPartition(tableName, new PatternEntryName(partitionPattern));
    }

    private Builder addBypassedPartition(String tableName, EntryName partition) {
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
     * Builds a {@link UdbFilterSpec}.
     * The builder should not be used after this method returns.
     * @return {@link UdbFilterSpec}
     * @throws IllegalArgumentException when invalid or conflicting configurations
     *                                  have been provided
     */
    public UdbFilterSpec build() {
      ImmutableMap.Builder<String, Pair<EntryMode, Set<EntryName>>> partitionsBuilder =
          ImmutableMap.builder();
      for (Map.Entry<String, Set<EntryName>> entry : mBypassedPartitions.entrySet()) {
        String tableName = entry.getKey();
        Set<EntryName> partitionEntryNames = entry.getValue();
        partitionsBuilder.put(tableName, new Pair<>(
            mPartitionBypassModes.getOrDefault(tableName, EntryMode.NONE), partitionEntryNames));
      }
      return new UdbFilterSpec(
          mBypassedTables,
          mTableBypassMode,
          partitionsBuilder.build(),
          mIgnoredTables,
          mIgnoreMode
      );
    }
  }
}
