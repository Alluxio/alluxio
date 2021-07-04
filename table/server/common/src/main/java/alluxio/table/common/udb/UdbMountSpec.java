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

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbMountSpec {
  /**
   * Map of table name to set of partition names.
   * Keyed by a table's name, the value set contains names of partitions in that table.
   * An empty set indicates all partitions of that table, if any, should be bypassed.
   */
  private final TablePartitionWrapper mBypassed;

  /**
   * Set of ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   */
  private final SimpleWrapper mIgnored;

  /**
   * @param bypassed bypassed table to partition map
   * @param ignored ignored tables set
   */
  public UdbMountSpec(TablePartitionWrapper bypassed,
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
   * when exclusion is not empty and inclusion is:
   * 1. empty: A2 + D is to be bypassed/ignored
   * 2. not empty: A2 is to be bypassed/ignored
   * when exclusion is empty and inclusion is:
   * 1. empty: none is to be bypassed/ignored
   * 2. not empty: A2 + B is to be bypassed/ignored
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
      return !mExcluded.has(name) && mIncluded.has(name);
    }
  }

  private static final class TablePartitionWrapper
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
      return !mExcluded.has(tableName) && mIncluded.hasFullTable(tableName);
    }

    boolean hasPartition(String tableName, String partName) {
      // if the whole table is excluded, so is any partition of that table
      return !mExcluded.has(tableName) && mIncluded.hasPartition(tableName, partName);
    }
  }

  /**
   * Type alias for {@link InclusionExclusionWrapper<SimpleNamePatternWrapper>}.
   */
  private static final class SimpleWrapper
      extends InclusionExclusionWrapper<SimpleNamePatternWrapper> {
    SimpleWrapper(SimpleNamePatternWrapper included, SimpleNamePatternWrapper excluded) {
      super(included, excluded);
    }
  }

  /**
   * Wrapper for table and partition specs, only appears in bypass-include list.
   */
  private static class TablePartitionNamePatternWrapper extends NamePatternWrapper {
    private final Map<String, Set<SimpleWrapper>> mTablePartMap;

    TablePartitionNamePatternWrapper(Set<String> fullTableNames, Set<Pattern> patterns) {
      this(fullTableNames, patterns, Collections.emptyMap());
    }

    TablePartitionNamePatternWrapper(Set<String> fullTableNames, Set<Pattern> patterns,
                                     Map<String, Set<SimpleWrapper>> tablePartMap) {
      super(fullTableNames, patterns);
      mTablePartMap = tablePartMap;
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
      if (getNames().contains(tableName)) {
        return true;
      }
      if (mTablePartMap.containsKey(tableName)) {
        // otherwise, check if it has an empty partition specification,
        // or a non-empty partition spec.
        // either way, it is explicitly listed, so no need to consult the regex patterns.
        return mTablePartMap.get(tableName).size() == 0;
      }
      // otherwise, check if it is covered by a regex pattern:
      return patternsMatch(tableName);
      // it's not listed in any way, so not a fully bypassed table.
    }

    @Override
    boolean has(String tableName) {
      return super.has(tableName) || mTablePartMap.containsKey(tableName);
    }

    /**
     * Alias of {@link TablePartitionNamePatternWrapper#has(String)}.
     */
    boolean hasTable(String tableName) {
      return has(tableName);
    }

    boolean hasPartition(String tableName, String partName) {
      if (hasFullTable(tableName)) {
        return true;
      }
      // not a fully bypassed table, this means:
      // 1. it's partially bypassed
      // 2. it's not bypassed at all
      Set<SimpleWrapper> simpleWrappers = mTablePartMap.get(tableName);
      if (simpleWrappers == null) {
        return false;
      }
      // no need to test if `simpleWrappers.size() == 0`, since we know the table is not
      // fully bypassed.
      return simpleWrappers.stream().anyMatch(p -> p.has(partName));
    }
  }

  /**
   * Type alias for NamePatternWrapper.
   */
  private static final class SimpleNamePatternWrapper extends NamePatternWrapper {
    SimpleNamePatternWrapper(Set<String> names, Set<Pattern> patterns) {
      super(names, patterns);
    }

    static SimpleNamePatternWrapper empty() {
      return new SimpleNamePatternWrapper(Collections.emptySet(), Collections.emptySet());
    }
  }

  abstract static class NamePatternWrapper {
    protected final Set<String> mNames;
    protected final Set<Pattern> mPatterns;

    NamePatternWrapper(Set<String> names, Set<Pattern> patterns) {
      mNames = names;
      mPatterns = patterns;
    }

    boolean isEmpty() {
      return mNames.size() == 0 && mPatterns.size() == 0;
    }

    boolean has(String name) {
      return mNames.contains(name) || patternsMatch(name);
    }

    Set<String> getNames() {
      return mNames;
    }

    Set<Pattern> getPatterns() {
      return mPatterns;
    }

    boolean patternsMatch(String target) {
      return mPatterns.stream().map(p -> p.matcher(target)).anyMatch(Matcher::matches);
    }
  }

  abstract static class BaseBuilder<T> {
    protected Set<String> mNames;
    protected Set<Pattern> mPatterns;

    protected BaseBuilder() {}

    public BaseBuilder<T> addName(String name) {
      mNames = addOrNewSet(mNames, name);
      return this;
    }

    public BaseBuilder<T> addPattern(Pattern pattern) {
      mPatterns = addOrNewSet(mPatterns, pattern);
      return this;
    }

    public BaseBuilder<T> addNames(Set<String> names) {
      mNames = addOrNewSet(mNames, names);
      return this;
    }

    public BaseBuilder<T> addPatterns(Set<Pattern> patterns) {
      mPatterns = addOrNewSet(mPatterns, patterns);
      return this;
    }

    public abstract T build();

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

  abstract static class IncludeExcludeBuilder<I, IBUILDERT extends BaseBuilder<I>,
                                              E, EBUILDERT extends BaseBuilder<E>,
                                              OUT> {
    private final IBUILDERT mInclude;
    private final EBUILDERT mExclude;

    protected IncludeExcludeBuilder(IBUILDERT includeBuilder, EBUILDERT excludeBuilder) {
      mInclude = includeBuilder;
      mExclude = excludeBuilder;
    }

    public IBUILDERT include() {
      return mInclude;
    }

    public EBUILDERT exclude() {
      return mExclude;
    }

    public abstract OUT build();
  }

  public static class SimpleNamePatternWrapperBuilder
      extends BaseBuilder<SimpleNamePatternWrapper> {
    @Override
    public SimpleNamePatternWrapper build() {
      mNames = BaseBuilder.replaceNullWithEmpty(mNames);
      mPatterns = BaseBuilder.replaceNullWithEmpty(mPatterns);
      SimpleNamePatternWrapper built = new SimpleNamePatternWrapper(mNames, mPatterns);
      // release references
      mNames = null;
      mPatterns = null;
      return built;
    }
  }

  public static class TablePartitionNamePatternWrapperBuilder
      extends BaseBuilder<TablePartitionNamePatternWrapper> {
    private Map<String, Set<SimpleWrapper>> mTablePartMap;

    public TablePartitionNamePatternWrapperBuilder addPartition(String tableName,
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
    public TablePartitionNamePatternWrapper build() {
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

  public static class TablePartitionWrapperBuilder extends IncludeExcludeBuilder<
      TablePartitionNamePatternWrapper,
      TablePartitionNamePatternWrapperBuilder,
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      TablePartitionWrapper> {

    protected TablePartitionWrapperBuilder(
        TablePartitionNamePatternWrapperBuilder includeBuilder,
        SimpleNamePatternWrapperBuilder excludeBuilder) {
      super(includeBuilder, excludeBuilder);
    }

    public TablePartitionWrapperBuilder() {
      this(new TablePartitionNamePatternWrapperBuilder(), new SimpleNamePatternWrapperBuilder());
    }

    @Override
    public TablePartitionWrapper build() {
      return new TablePartitionWrapper(include().build(), exclude().build());
    }
  }

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

    public SimpleWrapperBuilder() {
      this(new SimpleNamePatternWrapperBuilder(), new SimpleNamePatternWrapperBuilder());
    }

    @Override
    public SimpleWrapper build() {
      return new SimpleWrapper(include().build(), exclude().build());
    }
  }

  public static class Builder {
    private final TablePartitionWrapperBuilder mBypassBuilder;
    private final SimpleWrapperBuilder mIgnoreBuilder;

    public Builder() {
      mBypassBuilder = new TablePartitionWrapperBuilder();
      mIgnoreBuilder = new SimpleWrapperBuilder();
    }

    public TablePartitionWrapperBuilder bypass() {
      return mBypassBuilder;
    }

    public SimpleWrapperBuilder ignore() {
      return mIgnoreBuilder;
    }

    public UdbMountSpec build() {
      TablePartitionWrapper bypassed = mBypassBuilder.build();
      SimpleWrapper ignored = mIgnoreBuilder.build();
      return new UdbMountSpec(bypassed, ignored);
    }
  }
}
