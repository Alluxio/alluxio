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

import javax.annotation.Nullable;

/**
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbAttachOptions {
  private static final Logger LOG = LoggerFactory.getLogger(UdbAttachOptions.class);

  enum Entry {
    BYPASS, IGNORE, NONE;
  }

  enum Mode {
    INCLUDE, EXCLUDE, NONE;

    public Mode opposite() {
      if (this == INCLUDE) {
        return EXCLUDE;
      } else if (this == EXCLUDE) {
        return INCLUDE;
      } else {
        return NONE;
      }
    }
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
      @Nullable Set<Name> bypassedTables,
      @Nullable Mode tableBypassMode,
      @Nullable Map<String, Pair<Mode, Set<Name>>> bypassedPartitions,
      @Nullable Set<Name> ignoredTables,
      @Nullable Mode ignoreMode) {
    // ImmutableMap/Set.copyOf() ensures no null keys and values
    mBypassedTables = ImmutableSet.copyOf(replaceNullWithEmpty(bypassedTables));
    mIgnoredTables = ImmutableSet.copyOf(replaceNullWithEmpty(ignoredTables));
    mBypassedPartitions = ImmutableMap.copyOf(replaceNullWithEmpty(bypassedPartitions));

    if (mBypassedTables.isEmpty()) {
      mTableBypassMode = Mode.NONE;
    } else {
      Preconditions.checkArgument(tableBypassMode != null && tableBypassMode != Mode.NONE);
      mTableBypassMode = tableBypassMode;
    }
    if (mIgnoredTables.isEmpty()) {
      mIgnoreMode = Mode.NONE;
    } else {
      Preconditions.checkArgument(ignoreMode != null && ignoreMode != Mode.NONE);
      mIgnoreMode = ignoreMode;
    }
    for (Pair<Mode, Set<Name>> entry : mBypassedPartitions.values()) {
      Mode partitionMode = entry.getFirst();
      Set<Name> partitions = entry.getSecond();
      Preconditions.checkArgument(!partitions.isEmpty());
      Preconditions.checkArgument(partitionMode != null && partitionMode != Mode.NONE);
    }
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbAttachOptions#isFullyBypassedTable(String)
   */
  public boolean isBypassedTable(String tableName) {
    boolean isFullyBypassed = isContainedInWrappers(mBypassedTables, tableName);
    if (isFullyBypassed) {
      if (mTableBypassMode == Mode.EXCLUDE) {
        return false;
      }
      return shadowedByIgnore(tableName, true);
    }
    boolean hasBypassedPartition = mBypassedPartitions.containsKey(tableName);
    if (hasBypassedPartition) {
      // no need to check if mTableBypassMode here, bc if a table has partition specifications,
      // it must be include mode
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
    private Set<Name> mBypassedTables;
    private Set<Name> mIgnoredTables;
    private Map<String, Pair<Mode, Set<Name>>> mBypassedPartitions;
    private Mode mTableBypassMode;
    private Mode mIgnoreMode;

    private Entry mEntry;
    private Mode mMode;

    /**
     * Creates a new builder.
     */
    public Builder() {
      reset();
      create();
    }

    private void reset() {
      mEntry = Entry.NONE;
      mMode = Mode.NONE;
    }

    private void create() {
      mTableBypassMode = Mode.NONE;
      mIgnoreMode = Mode.NONE;
      mBypassedTables = new HashSet<>();
      mIgnoredTables = new HashSet<>();
      mBypassedPartitions = new HashMap<>();
    }

    /**
     * Set entry to bypass.
     * @return this builder
     */
    public Builder bypass() {
      reset();
      mEntry = Entry.BYPASS;
      return this;
    }

    /**
     * Set entry to ignore.
     * @return this builder
     */
    public Builder ignore() {
      reset();
      mEntry = Entry.IGNORE;
      return this;
    }

    /**
     * Set mode to include.
     * @return this builder
     */
    public Builder include() {
      mMode = Mode.INCLUDE;
      return this;
    }

    /**
     * Set mode to exclude.
     * @return this builder
     */
    public Builder exclude() {
      mMode = Mode.EXCLUDE;
      return this;
    }

    private void assertStateModeIsNotNone() {
      Preconditions.checkState(mMode != Mode.NONE,
          "Invalid inclusion/exclusion mode, must be either include or exclude");
    }

    private void assertStateEntryIsNotNone() {
      Preconditions.checkState(mEntry != Entry.NONE,
          "Invalid entry, must be either bypass or ignore");
    }

    /**
     * Add a table by its exact name.
     * @param name table name
     * @return this builder
     */
    public Builder addTable(String name) {
      return addTable(new ExactName(name));
    }

    /**
     * Add a table by pattern.
     * @param pattern pattern
     * @return this builder
     */
    public Builder addTable(Pattern pattern) {
      return addTable(new PatternName(pattern));
    }

    private Builder addTable(Name item) {
      assertStateModeIsNotNone();
      assertStateEntryIsNotNone();
      Mode opposite = mMode.opposite();
      switch (mEntry) {
        case BYPASS:
          if (mTableBypassMode == Mode.NONE) {
            mTableBypassMode = mMode;
          }
          Preconditions.checkState(mTableBypassMode == mMode,
              "Either inclusion or exclusion mode can be used but not both, "
              + "there are already bypassed tables in {} mode",
              mTableBypassMode);
          mBypassedTables.add(item);
          break;
        case IGNORE:
          if (mIgnoreMode == Mode.NONE) {
            mIgnoreMode = mMode;
          }
          Preconditions.checkState(mIgnoreMode == mMode,
              "Either inclusion or exclusion mode can be used but not both, "
                  + "there are already ignored tables in {} mode",
              mIgnoreMode);
          mIgnoredTables.add(item);
          break;
        case NONE:
        default:
          // unreachable
      }
      return this;
    }

    /**
     * Add multiple tables at once.
     * @param items set of tables. contained elements must be either strings or patterns
     * @return this builder
     */
    public Builder addTables(Set<Object> items) {
      items.forEach((item) -> {
        Name wrapper;
        if (item instanceof String) {
          wrapper = new ExactName((String) item);
        } else if (item instanceof Pattern) {
          wrapper = new PatternName((Pattern) item);
        } else {
          throw new IllegalArgumentException("Table name must be string or pattern");
        }
        addTable(wrapper);
      });
      return this;
    }

    /**
     * Add a partition by its exact name.
     * @param tableName the containing table
     * @param partitionName partition name
     * @return this builder
     */
    public Builder addPartition(String tableName, String partitionName) {
      return addPartition(tableName, new ExactName(partitionName));
    }

    /**
     * Add a partition by pattern.
     * @param tableName the containing table
     * @param partitionPattern partition pattern
     * @return this builder
     */
    public Builder addPartition(String tableName, Pattern partitionPattern) {
      return addPartition(tableName, new PatternName(partitionPattern));
    }

    private Builder addPartition(String tableName, Name partition) {
      assertStateModeIsNotNone();
      assertStateEntryIsNotNone();
      // forbid adding partition to ignored tables
      Preconditions.checkState(mEntry == Entry.BYPASS,
          "Ignored table {} cannot have partition specifications",
          tableName);
      // forbid including and excluding partitions for a table at the same time
      Preconditions.checkState(!(mBypassedPartitions.containsKey(tableName)
          && mBypassedPartitions.get(tableName).getFirst() == mMode.opposite()),
          "Either inclusion or exclusion mode can be used but not both, "
              + "table {} already has {} partitions",
          tableName,
          mMode.opposite());
      // forbid adding partitions when tables are excluded
      Preconditions.checkState(mTableBypassMode != Mode.EXCLUDE,
          "Table {} is specified with exclusion mode, "
              + "cannot have partition specification",
          tableName);
      // forbid both plain name and partition spec for a table at the same time
      Preconditions.checkState(mBypassedTables.stream()
          .noneMatch(wrapper -> wrapper instanceof ExactName && wrapper.matches(tableName)),
          "Table {} is already specified with exact name, "
              + "ignoring any partition specifications",
          tableName);

      mBypassedPartitions.compute(tableName, (tblName, pairOfModeSet) -> {
        if (pairOfModeSet == null) {
          return new Pair<>(mMode, Sets.newHashSet(partition));
        } else {
          pairOfModeSet.getSecond().add(partition);
          return pairOfModeSet;
        }
      });
      return this;
    }

    /**
     * Build a {@link UdbAttachOptions}.
     * @return {@link UdbAttachOptions}
     */
    public UdbAttachOptions build() {
      UdbAttachOptions built = new UdbAttachOptions(
          mBypassedTables,
          mTableBypassMode,
          mBypassedPartitions,
          mIgnoredTables,
          mIgnoreMode
      );
      reset();
      create();
      return built;
    }
  }
}
