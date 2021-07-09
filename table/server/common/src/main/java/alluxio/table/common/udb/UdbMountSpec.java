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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbMountSpec {
  /**
   * Tables and partitions to bypass.
   */
  private final TablePartitionWrapper mBypassed;

  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   */
  private final SimpleWrapper mIgnored;

  protected UdbMountSpec(TablePartitionWrapper bypassed,
                         SimpleWrapper ignored) {
    mBypassed = bypassed;
    mIgnored = ignored;
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbMountSpec#hasFullyBypassedTable(String)
   */
  public boolean hasBypassedTable(String tableName) {
    return mBypassed.hasTable(tableName);
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbMountSpec#hasBypassedTable(String)
   */
  public boolean hasFullyBypassedTable(String tableName) {
    return mBypassed.hasFullTable(tableName);
  }

  /**
   * Checks if a table is ignored.
   *
   * @param tableName the table name
   * @return true if the table is ignored, false otherwise
   */
  public boolean hasIgnoredTable(String tableName) {
    return mIgnored.has(tableName);
  }

  /**
   * Checks by a partition's name if it should be bypassed.
   *
   * @param tableName the table name
   * @param partitionName the partition name
   * @return true if the partition should be bypassed, false otherwise
   */
  public boolean hasBypassedPartition(String tableName, String partitionName) {
    return mBypassed.hasPartition(tableName, partitionName);
  }

  /**
   * Wrapper class that takes into account inclusion and exclusion behaviour.
   *  |-----------------------|
   *  |  inclusion            |       All items
   *  |         |-------------|------|
   *  |         |    A2       |  D2  |
   *  |    A1   |     |-------|------|--------|
   *  |         |     |   B   |      |        |
   *  |_________|_____|_______|      |        |
   *            |  D1 |       C2     |        |
   *            |_____|______________|   C1   |
   *                  |           exclusion   |
   *                  |_______________________|
   * D = D1 + D2
   *
   * General rules:
   * when exclusion is not empty and inclusion is:
   * 1. empty: A2 + D is to be bypassed/ignored
   * 2. not empty: A2 is to be bypassed/ignored
   * when exclusion is empty and inclusion is:
   * 1. empty: none is to be bypassed/ignored
   * 2. not empty: A2 + B is to be bypassed/ignored
   *
   * Corner cases:
   * 1. Explicit names have priority over patterns.
   *     If a table is included by an explicit name but excluded by a pattern,
   *     then it is still considered included.
   *     Likewise, if it is excluded by an explicit name but included by a pattern,
   *     it's still excluded.
   * 2. If a name is specified explicitly both in the include list and the exclude list,
   *     then it's an IllegalStateException.
   * 3. If a name is covered by both the include and the exclude patterns,
   *     then it's excluded. (or included? or IllegalStateException?)
   */
  abstract static class InclusionExclusionWrapper<T extends NamePatternWrapper> {
    protected final T mIncluded;
    // excluded entries don't allow partition specifications -- they are name and regex only
    protected final SimpleNamePatternWrapper mExcluded;

    InclusionExclusionWrapper(T included, SimpleNamePatternWrapper excluded) {
      mIncluded = included;
      mExcluded = excluded;
    }

    // generic implementation
    boolean has(String name) {
      if (mExcluded.isEmpty()) {
        if (mIncluded.isEmpty()) {
          return false;
        } else {
          return mIncluded.has(name);
        }
      } else {
        if (mIncluded.isEmpty()) {
          // when include is empty, anything that is not excluded is implicitly included
          return !mExcluded.has(name);
        } else {
          return handleExplicitness(name);
        }
      }
    }

    protected boolean handleExplicitness(String name) {
      return handleExplicitness(
          name,
          mIncluded.hasExplicit(name),
          mExcluded.hasExplicit(name),
          mIncluded.hasCoveredByPattern(name),
          mExcluded.hasCoveredByPattern(name));
    }

    protected boolean handleExplicitness(
        String name,
        boolean isExplicitlyIncluded,
        boolean isExplicitlyExcluded,
        boolean isIncludedByPattern,
        boolean isExcludedByPattern
    ) {
      if (isExplicitlyIncluded && isExplicitlyExcluded) {
        throw new IllegalStateException(
            String.format("Name `%s` is both included and excluded explicitly", name));
      }
      if (isExplicitlyIncluded) {
        return true;
      }
      if (isExplicitlyExcluded) {
        return false;
      }
      if (isIncludedByPattern && isExcludedByPattern) {
        return false;
      }
      if (isIncludedByPattern) {
        return true;
      }
      // isExcludedByPattern == true
      return false;
    }
  }

  static final class TablePartitionWrapper
      extends InclusionExclusionWrapper<TablePartitionNamePatternWrapper> {
    TablePartitionWrapper(
        TablePartitionNamePatternWrapper included,
        SimpleNamePatternWrapper excluded) {
      super(included, excluded);
    }

    boolean hasTable(String tableName) {
      return has(tableName);
    }

    boolean hasFullTable(String tableName) {
      boolean isFullTableIncludedExplicitly =
          mIncluded.hasExplicit(tableName) && mIncluded.hasFullTable(tableName);
      boolean isFullTableIncludedByPattern =
          mIncluded.hasCoveredByPattern(tableName) && mIncluded.hasFullTable(tableName);
      boolean isTableExcludedExplicitly = mExcluded.hasExplicit(tableName);
      boolean isTableExcludedByPattern = mExcluded.hasCoveredByPattern(tableName);

      if (mExcluded.isEmpty()) {
        if (mIncluded.isEmpty()) {
          return false;
        } else {
          return mIncluded.hasFullTable(tableName);
        }
      } else {
        if (mIncluded.isEmpty()) {
          // implicitly included tables are fully bypassed
          return !mExcluded.has(tableName);
        } else {
          return handleExplicitness(
              tableName,
              isFullTableIncludedExplicitly,
              isTableExcludedExplicitly,
              isFullTableIncludedByPattern,
              isTableExcludedByPattern
          );
        }
      }
    }

    boolean hasPartition(String tableName, String partName) {
      if (mExcluded.isEmpty()) {
        if (mIncluded.isEmpty()) {
          return false;
        } else {
          return mIncluded.hasPartition(tableName, partName);
        }
      } else {
        if (mIncluded.isEmpty()) {
          // implicitly included tables are fully bypassed
          return !mExcluded.has(tableName);
        } else {
          // first check if the table is present
          boolean hasTable = handleExplicitness(tableName);
          // then checks if the partition is present
          return hasTable && mIncluded.hasPartition(tableName, partName);
        }
      }
    }
  }

  /**
   * Type alias for {@link InclusionExclusionWrapper<SimpleNamePatternWrapper>}.
   */
  static final class SimpleWrapper extends InclusionExclusionWrapper<SimpleNamePatternWrapper> {
    SimpleWrapper(SimpleNamePatternWrapper included, SimpleNamePatternWrapper excluded) {
      super(included, excluded);
    }
  }

  /**
   * Wrapper for table and partition specs, only appears in bypass-include list.
   */
  static class TablePartitionNamePatternWrapper extends NamePatternWrapper {
    private final ImmutableMap<String, Set<SimpleWrapper>> mTablePartMap;

    TablePartitionNamePatternWrapper(Set<String> fullTableNames, Set<Pattern> patterns) {
      this(fullTableNames, patterns, Collections.emptyMap());
    }

    TablePartitionNamePatternWrapper(Set<String> fullTableNames, Set<Pattern> patterns,
                                     Map<String, Set<SimpleWrapper>> tablePartMap) {
      super(fullTableNames, patterns);
      mTablePartMap = ImmutableMap.copyOf(tablePartMap);
    }

    static TablePartitionNamePatternWrapper empty() {
      return new TablePartitionNamePatternWrapper(Collections.emptySet(), Collections.emptySet());
    }

    @Override
    boolean isEmpty() {
      return super.isEmpty() && mTablePartMap.isEmpty();
    }

    boolean hasFullTable(String tableName) {
      // if a table is listed with an explicit literal name, then the partition specification
      // or regex patterns should not be consulted:
      // here use parent's hasExplicit since self's takes into account partition specs
      if (super.hasExplicit(tableName)) {
        return true;
      }
      if (mTablePartMap.containsKey(tableName)) {
        // otherwise, check if it has an empty partition specification,
        // or a non-empty partition spec.
        // either way, it is explicitly listed, so no need to consult the regex patterns.
        return mTablePartMap.get(tableName).size() == 0;
      }
      // otherwise, check if it is covered by a regex pattern:
      return hasCoveredByPattern(tableName);
      // it's not listed in any way, so not a fully bypassed table.
    }

    @Override
    boolean has(String tableName) {
      return super.has(tableName) || mTablePartMap.containsKey(tableName);
    }

    @Override
    boolean hasExplicit(String tableName) {
      return super.hasExplicit(tableName) || mTablePartMap.containsKey(tableName);
    }

    /**
     * Alias of {@link TablePartitionNamePatternWrapper#has(String)}.
     */
    boolean hasTable(String tableName) {
      return has(tableName);
    }

    boolean hasPartition(String tableName, String partName) {
      // any partition of a fully bypassed table is bypassed
      if (hasFullTable(tableName)) {
        return true;
      }
      // otherwise, one of the following is true:
      // 1. the table's partially bypassed
      // 2. it's not bypassed at all
      Set<SimpleWrapper> simpleWrappers = mTablePartMap.get(tableName);
      if (simpleWrappers == null) {
        // no partition specs, so it's not bypassed at all
        return false;
      }
      // it's partially bypassed
      // here we know for sure `simpleWrappers.size() != 0`
      return simpleWrappers.stream().anyMatch(p -> p.has(partName));
    }
  }

  /**
   * Type alias for NamePatternWrapper.
   */
  static final class SimpleNamePatternWrapper extends NamePatternWrapper {
    SimpleNamePatternWrapper(Set<String> names, Set<Pattern> patterns) {
      super(names, patterns);
    }

    static SimpleNamePatternWrapper empty() {
      return new SimpleNamePatternWrapper(Collections.emptySet(), Collections.emptySet());
    }
  }

  abstract static class NamePatternWrapper {
    protected final ImmutableSet<String> mNames;
    protected final ImmutableSet<Pattern> mPatterns;

    NamePatternWrapper(Set<String> names, Set<Pattern> patterns) {
      mNames = ImmutableSet.copyOf(names);
      mPatterns = ImmutableSet.copyOf(patterns);
    }

    boolean isEmpty() {
      return mNames.size() == 0 && mPatterns.size() == 0;
    }

    boolean has(String name) {
      return hasExplicit(name) || hasCoveredByPattern(name);
    }

    boolean hasExplicit(String name) {
      return mNames.contains(name);
    }

    boolean hasCoveredByPattern(String name) {
      return mPatterns.stream().map(p -> p.matcher(name)).anyMatch(Matcher::matches);
    }

    Set<String> getNames() {
      return mNames;
    }

    Set<Pattern> getPatterns() {
      return mPatterns;
    }
  }

  /**
   * Base builder for common builder methods.
   * @param <T> the output type
   */
  public abstract static class BaseBuilder<T> {
    @Nullable
    protected Set<String> mNames;
    @Nullable
    protected Set<Pattern> mPatterns;

    protected BaseBuilder() {}

    /**
     * Adds a name.
     * @param name name
     * @return the builder
     */
    public BaseBuilder<T> addName(String name) {
      mNames = addOrNewSet(mNames, name);
      return this;
    }

    /**
     * Adds a pattern.
     * @param pattern pattern
     * @return the builder
     */
    public BaseBuilder<T> addPattern(Pattern pattern) {
      mPatterns = addOrNewSet(mPatterns, pattern);
      return this;
    }

    /**
     * Adds a set of names, the set has to be mutable.
     * @param names set of names
     * @return the builder
     */
    public BaseBuilder<T> addNames(Set<String> names) {
      mNames = addOrNewSet(mNames, names);
      return this;
    }

    /**
     * Adds a set of patterns, the set has to be mutable.
     * @param patterns set of names
     * @return the builder
     */
    public BaseBuilder<T> addPatterns(Set<Pattern> patterns) {
      mPatterns = addOrNewSet(mPatterns, patterns);
      return this;
    }

    /**
     * Builds the type.
     * @return the built type
     */
    abstract T build();

    private static <T> Set<T> addOrNewSet(Set<T> someSet, T element) {
      if (someSet == null) {
        Set<T> newSet = new HashSet<>();
        newSet.add(element);
        return newSet;
      } else {
        someSet.add(element);
        return someSet;
      }
    }

    private static <T> Set<T> addOrNewSet(Set<T> someSet, Set<T> elements) {
      if (someSet == null) {
        return elements;
      } else {
        someSet.addAll(elements);
        return someSet;
      }
    }

    private static <T> Set<T> replaceNullWithEmpty(Set<T> someSet) {
      if (someSet == null) {
        return Collections.emptySet();
      }
      return someSet;
    }
  }

  /**
   * Builder for include/exclude list.
   * @param <I> the type contained in the included list
   * @param <IBUILDERT> builder type for {@link I}
   * @param <E> the type contained in the excluded list
   * @param <EBUILDERT> builder type for {@link E}
   * @param <OUT> the output type
   */
  abstract static class IncludeExcludeBuilder<I, IBUILDERT extends BaseBuilder<I>,
                                              E, EBUILDERT extends BaseBuilder<E>,
                                              OUT> {
    private final IBUILDERT mInclude;
    private final EBUILDERT mExclude;

    protected IncludeExcludeBuilder(IBUILDERT includeBuilder, EBUILDERT excludeBuilder) {
      mInclude = includeBuilder;
      mExclude = excludeBuilder;
    }

    /**
     * Returns the included entry's builder.
     */
    public IBUILDERT include() {
      return mInclude;
    }

    /**
     * Returns the excluded entry's builder.
     */
    public EBUILDERT exclude() {
      return mExclude;
    }

    /**
     * Builds the type.
     */
    abstract OUT build();
  }

  /**
   * Builder for simple names and patterns.
   */
  public static class SimpleNamePatternWrapperBuilder
      extends BaseBuilder<SimpleNamePatternWrapper> {
    @Override
    SimpleNamePatternWrapper build() {
      mNames = BaseBuilder.replaceNullWithEmpty(mNames);
      mPatterns = BaseBuilder.replaceNullWithEmpty(mPatterns);
      SimpleNamePatternWrapper built = new SimpleNamePatternWrapper(mNames, mPatterns);
      // release references
      mNames = null;
      mPatterns = null;
      return built;
    }
  }

  /**
   * Builder for table with partition specification.
   */
  public static class TablePartitionNamePatternWrapperBuilder
      extends BaseBuilder<TablePartitionNamePatternWrapper> {
    private Map<String, Set<SimpleWrapper>> mTablePartMap;

    /**
     * Attaches partition specification to a table.
     * @param tableName the table name
     * @param builder builder for partition specification
     * @return the builder
     */
    public TablePartitionNamePatternWrapperBuilder addPartition(String tableName,
                                                                SimpleWrapperBuilder builder) {
      return addPartition(tableName, builder.build());
    }

    TablePartitionNamePatternWrapperBuilder addPartition(String tableName,
                                                         SimpleWrapper partition) {
      if (mTablePartMap == null) {
        mTablePartMap = new HashMap<>();
      }
      Set<SimpleWrapper> set = mTablePartMap.get(tableName);
      if (set != null) {
        set.add(partition);
      } else {
        mTablePartMap.put(tableName, Sets.newHashSet(partition));
      }
      return this;
    }

    @Override
    TablePartitionNamePatternWrapper build() {
      mNames = BaseBuilder.replaceNullWithEmpty(mNames);
      mPatterns = BaseBuilder.replaceNullWithEmpty(mPatterns);
      mTablePartMap = mTablePartMap == null ? Collections.emptyMap() : mTablePartMap;
      TablePartitionNamePatternWrapper built =
          new TablePartitionNamePatternWrapper(mNames, mPatterns, mTablePartMap);
      // release references
      mNames = null;
      mPatterns = null;
      mTablePartMap = null;
      return built;
    }
  }

  /**
   * Builder for the bypass entry.
   */
  public static class BypassEntryBuilder extends IncludeExcludeBuilder<
      TablePartitionNamePatternWrapper,
      TablePartitionNamePatternWrapperBuilder,
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      TablePartitionWrapper> {

    protected BypassEntryBuilder(
        TablePartitionNamePatternWrapperBuilder includeBuilder,
        SimpleNamePatternWrapperBuilder excludeBuilder) {
      super(includeBuilder, excludeBuilder);
    }

    /**
     * Creates a new builder.
     */
    public BypassEntryBuilder() {
      this(new TablePartitionNamePatternWrapperBuilder(), new SimpleNamePatternWrapperBuilder());
    }

    @Override
    TablePartitionWrapper build() {
      return new TablePartitionWrapper(include().build(), exclude().build());
    }
  }

  /**
   * Builder for the ignore entry.
   */
  public static class SimpleWrapperBuilder extends IncludeExcludeBuilder<
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      SimpleWrapper> {

    protected SimpleWrapperBuilder(
        SimpleNamePatternWrapperBuilder includeBuilder,
        SimpleNamePatternWrapperBuilder excludeBuilder) {
      super(includeBuilder, excludeBuilder);
    }

    /**
     * Creates a new builder.
     */
    public SimpleWrapperBuilder() {
      this(new SimpleNamePatternWrapperBuilder(), new SimpleNamePatternWrapperBuilder());
    }

    @Override
    public SimpleWrapper build() {
      return new SimpleWrapper(include().build(), exclude().build());
    }
  }

  /**
   * Builder for {@link UdbMountSpec}.
   */
  public static class Builder {
    private final BypassEntryBuilder mBypassBuilder;
    private final SimpleWrapperBuilder mIgnoreBuilder;

    /**
     * Creates a new builder.
     */
    public Builder() {
      mBypassBuilder = new BypassEntryBuilder();
      mIgnoreBuilder = new SimpleWrapperBuilder();
    }

    /**
     * Gets builder for the bypass entry.
     * @return the builder
     */
    public BypassEntryBuilder bypass() {
      return mBypassBuilder;
    }

    /**
     * Gets Builder for the ignore entry.
     * @return the builder
     */
    public SimpleWrapperBuilder ignore() {
      return mIgnoreBuilder;
    }

    /**
     * Builds the spec.
     * @return the builder
     */
    public UdbMountSpec build() {
      TablePartitionWrapper bypassed = mBypassBuilder.build();
      SimpleWrapper ignored = mIgnoreBuilder.build();
      return new UdbMountSpec(bypassed, ignored);
    }
  }
}
