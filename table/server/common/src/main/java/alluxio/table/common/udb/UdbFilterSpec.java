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
   * Bypass/ignore entries.
   */
  public enum EntryType {
    BYPASS, IGNORE, NONE;
  }

  /**
   * Inclusion/exclusion modes of tables and partitions.
   */
  public enum EntryMode {
    INCLUDE, EXCLUDE, NONE;
  }

  /**
   * Bypassed tables.
   * When this set is empty, mTableBypassMode is set to NONE.
   */
  private final Set<NameTemplate> mBypassedTables;
  /**
   * Mode of bypassed tables: included or excluded.
   */
  private final EntryMode mTableBypassMode;
  /**
   * Map of table names to their bypassed partitions and mode.
   */
  private final Map<String, Pair<EntryMode, Set<NameTemplate>>> mBypassedPartitions;
  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   * When this set is empty, mIgnoreMode is set to NONE.
   */
  private final Set<NameTemplate> mIgnoredTables;
  /**
   * Mode of ignored tables: included or excluded.
   */
  private final EntryMode mIgnoreMode;

  private UdbFilterSpec(
      Set<NameTemplate> bypassedTables,
      EntryMode tableBypassMode,
      Map<String, Pair<EntryMode, Set<NameTemplate>>> bypassedPartitions,
      Set<NameTemplate> ignoredTables,
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
      for (Map.Entry<String, Pair<EntryMode, Set<NameTemplate>>> entry :
          mBypassedPartitions.entrySet()) {
        String tableName = entry.getKey();
        EntryMode partitionMode = entry.getValue().getFirst();
        Set<NameTemplate> partitionNameTemplates = entry.getValue().getSecond();
        // forbid empty table name
        Preconditions.checkArgument(!tableName.isEmpty(),
            "Empty table name specified with partition specification %s",
            partitionNameTemplates);
        // forbid empty spec
        Preconditions.checkArgument(!partitionNameTemplates.isEmpty(),
            "Empty partition specification set for table %s",
            tableName);
        // forbid NONE mode
        Preconditions.checkArgument(partitionMode != EntryMode.NONE,
            "Invalid mode %s for partitions of table %s, must be either include or exclude",
            partitionMode, tableName);
        // forbid both exact name and partition spec for a table at the same time
        Preconditions.checkArgument(mBypassedTables.stream()
                .noneMatch(template -> template instanceof ExactNameTemplate
                    && template.matches(tableName)),
            "Table %s is already specified with exact name, "
                + "cannot have partition specification at the same time",
            tableName);
      }
    }
  }

  public static UdbFilterSpec empty() {
    return EMPTY_INSTANCE;
  }

  /**
   * Checks if a table should be bypassed.
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
   * Checks if all partitions of a table should be bypassed.
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
    EntryMode partitionMode = mBypassedPartitions.get(tableName).getFirst();
    boolean isBypassed =
        matchedByTemplates(mBypassedPartitions.get(tableName).getSecond(), partitionName);
    return shadowedByIgnore(tableName, handleInclusionExclusion(partitionMode, isBypassed));
  }

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
      // todo(bowen): this warning is never triggered bc if a table is ignored,
      // it will not be checked for bypassing, given the current implementation in Database
      LOG.warn("Table {} is set to be bypassed but it is also ignored", tableName);
      return false;
    }
    return mightBeShadowedIfTrue;
  }

  private static boolean matchedByTemplates(Set<NameTemplate> templates, String name) {
    return templates.stream().anyMatch(p -> p.matches(name));
  }

  /**
   * Abstract name for tables and partitions.
   */
  interface NameTemplate {
    /**
     * Checks whether the argument name matches the configured name pattern.
     *
     * @param name the concrete name to check for
     * @return true if the names match, false otherwise
     */
    boolean matches(String name);
  }

  static class ExactNameTemplate implements NameTemplate {
    private final String mExactName;

    public ExactNameTemplate(String exactName) {
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
      ExactNameTemplate that = (ExactNameTemplate) o;
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

  static class PatternNameTemplate implements NameTemplate {
    private final Pattern mPattern;

    public PatternNameTemplate(Pattern pattern) {
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
      PatternNameTemplate that = (PatternNameTemplate) o;
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
    private Set<NameTemplate> mBypassedTables;
    private Set<NameTemplate> mIgnoredTables;
    private Map<String, Set<NameTemplate>> mBypassedPartitions;
    private Map<String, EntryMode> mPartitionBypassModes;
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
     * Include or exclude the bypassed tables.
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setBypassedTablesMode(EntryMode entryMode) {
      mTableBypassMode = entryMode;
      return this;
    }

    /**
     * Include or exclude the ignored tables.
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setIgnoredTablesMode(EntryMode entryMode) {
      mIgnoreMode = entryMode;
      return this;
    }

    /**
     * Add a table by its exact name to the bypass entry.
     * @param name table name
     * @return this builder
     */
    public Builder addBypassedTable(String name) {
      return addTable(new ExactNameTemplate(name), EntryType.BYPASS);
    }

    /**
     * Add a table by pattern to the bypass entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addBypassedTable(Pattern pattern) {
      return addTable(new PatternNameTemplate(pattern), EntryType.BYPASS);
    }

    /**
     * Add a table by its exact name to the ignore entry.
     * @param name table name
     * @return this builder
     */
    public Builder addIgnoredTable(String name) {
      return addTable(new ExactNameTemplate(name), EntryType.IGNORE);
    }

    /**
     * Add a table by pattern to the ignore entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addIgnoredTable(Pattern pattern) {
      return addTable(new PatternNameTemplate(pattern), EntryType.IGNORE);
    }

    private Builder addTable(NameTemplate item, EntryType entryType) {
      switch (entryType) {
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
     * @param entryMode {@link EntryMode} of the tables
     * @return this builder
     */
    public Builder setBypassedPartitionsMode(String tableName, EntryMode entryMode) {
      mPartitionBypassModes.put(tableName, entryMode);
      return this;
    }

    /**
     * Add a partition by its exact name.
     * @param tableName the containing table
     * @param partitionName partition name
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, String partitionName) {
      return addBypassedPartition(tableName, new ExactNameTemplate(partitionName));
    }

    /**
     * Add a partition by pattern.
     * @param tableName the containing table
     * @param partitionPattern partition pattern
     * @return this builder
     */
    public Builder addBypassedPartition(String tableName, Pattern partitionPattern) {
      return addBypassedPartition(tableName, new PatternNameTemplate(partitionPattern));
    }

    private Builder addBypassedPartition(String tableName, NameTemplate partition) {
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
     * Build a {@link UdbFilterSpec}.
     * The builder should not be used after this method returns.
     * @return {@link UdbFilterSpec}
     * @throws IllegalArgumentException when invalid or conflicting configurations
     *                                          have been provided
     */
    public UdbFilterSpec build() {
      ImmutableMap.Builder<String, Pair<EntryMode, Set<NameTemplate>>> partitionsBuilder
          = ImmutableMap.builder();
      for (Map.Entry<String, Set<NameTemplate>> entry : mBypassedPartitions.entrySet()) {
        String tableName = entry.getKey();
        Set<NameTemplate> partitionNameTemplates = entry.getValue();
        partitionsBuilder.put(tableName, new Pair<>(
            mPartitionBypassModes.getOrDefault(tableName, EntryMode.NONE), partitionNameTemplates));
      }
      UdbFilterSpec built = new UdbFilterSpec(
          mBypassedTables,
          mTableBypassMode,
          partitionsBuilder.build(),
          mIgnoredTables,
          mIgnoreMode
      );
      // release references to prevent reuse of builder
      mBypassedTables = ImmutableSet.of();
      mTableBypassMode = EntryMode.NONE;
      mBypassedPartitions = ImmutableMap.of();
      mPartitionBypassModes = ImmutableMap.of();
      mIgnoredTables = ImmutableSet.of();
      mIgnoreMode = EntryMode.NONE;
      return built;
    }
  }
}
