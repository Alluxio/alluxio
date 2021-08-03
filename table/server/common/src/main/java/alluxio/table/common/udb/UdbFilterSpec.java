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
  private final Set<NameTemplate> mBypassedTables;
  /**
   * Mode of bypassed tables: included or excluded.
   */
  private final Mode mTableBypassMode;
  /**
   * Map of table names to their bypassed partitions and mode.
   */
  private final Map<String, Pair<Mode, Set<NameTemplate>>> mBypassedPartitions;
  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   * When this set is empty, mIgnoreMode is set to NONE.
   */
  private final Set<NameTemplate> mIgnoredTables;
  /**
   * Mode of ignored tables: included or excluded.
   */
  private final Mode mIgnoreMode;

  private UdbFilterSpec(
      Set<NameTemplate> bypassedTables,
      Mode tableBypassMode,
      Map<String, Pair<Mode, Set<NameTemplate>>> bypassedPartitions,
      Set<NameTemplate> ignoredTables,
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
   * @see UdbFilterSpec#isFullyBypassedTable(String)
   */
  public boolean isBypassedTable(String tableName) {
    boolean isSpecifiedByName = matchedByTemplates(mBypassedTables, tableName);
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
    Mode partitionMode = mBypassedPartitions.get(tableName).getFirst();
    boolean isBypassed =
        matchedByTemplates(mBypassedPartitions.get(tableName).getSecond(), partitionName);
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
    private final Set<NameTemplate> mBypassedTables;
    private final Set<NameTemplate> mIgnoredTables;
    private final Map<String, Set<NameTemplate>> mBypassedPartitions;
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
      return addTable(new ExactNameTemplate(name), Entry.BYPASS);
    }

    /**
     * Add a table by pattern to the bypass entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addBypassedTable(Pattern pattern) {
      return addTable(new PatternNameTemplate(pattern), Entry.BYPASS);
    }

    /**
     * Add a table by its exact name to the ignore entry.
     * @param name table name
     * @return this builder
     */
    public Builder addIgnoredTable(String name) {
      return addTable(new ExactNameTemplate(name), Entry.IGNORE);
    }

    /**
     * Add a table by pattern to the ignore entry.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addIgnoredTable(Pattern pattern) {
      return addTable(new PatternNameTemplate(pattern), Entry.IGNORE);
    }

    private Builder addTable(NameTemplate item, Entry entry) {
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
     * @return {@link UdbFilterSpec}
     */
    public UdbFilterSpec build() {
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
      Map<String, Pair<Mode, Set<NameTemplate>>> partitions = new HashMap<>();
      if (!mBypassedPartitions.isEmpty()) {
        // forbid adding partitions when tables are excluded
        Preconditions.checkState(mTableBypassMode != Mode.EXCLUDE,
            "Tables are specified with exclusion mode, cannot have partition specification");
        for (Map.Entry<String, Set<NameTemplate>> entry : mBypassedPartitions.entrySet()) {
          String tableName = entry.getKey();
          Set<NameTemplate> partitionNameTemplates = entry.getValue();
          // make sure this table has associated partition mode
          Preconditions.checkState(mPartitionBypassModes.containsKey(tableName)
              && mPartitionBypassModes.get(tableName) != Mode.NONE,
              "Missing partitions mode for table {}",
              tableName);
          // forbid both exact name and partition spec for a table at the same time
          Preconditions.checkState(mBypassedTables.stream()
                  .noneMatch(template -> template instanceof ExactNameTemplate
                      && template.matches(tableName)),
              "Table {} is already specified with exact name, "
                  + "cannot have partition specification at the same time",
              tableName);
          Preconditions.checkState(!partitionNameTemplates.isEmpty(),
              "Empty partition specification set for table {}",
              tableName);
          partitions.put(tableName,
              new Pair<>(mPartitionBypassModes.get(tableName), partitionNameTemplates));
        }
      }
      return new UdbFilterSpec(
          mBypassedTables,
          mTableBypassMode,
          partitions,
          mIgnoredTables,
          mIgnoreMode
      );
    }
  }
}
