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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Tables and partitions inclusion and exclusion specification.
 */
public final class UdbAttachSpec {
  private static final Logger LOG = LoggerFactory.getLogger(UdbAttachSpec.class);

  /**
   * Tables and partitions to bypass.
   */
  private final TablePartitionInclusionExclusionWrapper mBypassed;

  /**
   * Ignored tables.
   * Tables are ignored as a whole, no partition configuration is needed.
   */
  private final NamePatternInclusionExclusionWrapper mIgnored;

  protected UdbAttachSpec(TablePartitionInclusionExclusionWrapper bypassed,
                          NamePatternInclusionExclusionWrapper ignored) {
    mBypassed = bypassed;
    mIgnored = ignored;
  }

  /**
   * Checks if a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be bypassed, false otherwise
   * @see UdbAttachSpec#hasFullyBypassedTable(String)
   */
  public boolean hasBypassedTable(String tableName) {
    return shadowedByIgnore(tableName, mBypassed.hasTable(tableName));
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbAttachSpec#hasBypassedTable(String)
   */
  public boolean hasFullyBypassedTable(String tableName) {
    return shadowedByIgnore(tableName, mBypassed.hasFullTable(tableName));
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
    return shadowedByIgnore(tableName, mBypassed.hasPartition(tableName, partitionName));
  }

  /**
   * Checks if a table is shadowed by ignoring when it is also bypassed.
   * @param tableName the table name
   * @param mightBeShadowedIfTrue the return value of {@link #hasBypassedTable(String)}, etc
   * @return false if the table is shadowed by ignoring, mightBeShadowedIfTrue if it is not
   */
  private boolean shadowedByIgnore(String tableName, boolean mightBeShadowedIfTrue) {
    if (mightBeShadowedIfTrue && hasIgnoredTable(tableName)) {
      LOG.warn("Table {} is set to be bypassed but it is also ignored", tableName);
      return false;
    }
    return mightBeShadowedIfTrue;
  }

  /**
   * Wrapper class that takes into account inclusion and exclusion behaviour.
   */
  abstract static class InclusionExclusionWrapper<T extends NamePatternWrapper> {
    protected final T mIncluded;
    // excluded entries don't allow partition specifications -- they are name and regex only
    protected final SimpleNamePatternWrapper mExcluded;

    InclusionExclusionWrapper(T included, SimpleNamePatternWrapper excluded) {
      // currently either included or excluded list is accepted, but not both
      Preconditions.checkArgument(included.isEmpty() || excluded.isEmpty());
      mIncluded = included;
      mExcluded = excluded;
    }

    // generic implementation
    boolean has(String name) {
      if (mExcluded.isEmpty()) {
        return mIncluded.has(name);
      } else {
        return !mExcluded.has(name);
      }
    }
  }

  static final class TablePartitionInclusionExclusionWrapper
      extends InclusionExclusionWrapper<TablePartitionNamePatternWrapper> {
    TablePartitionInclusionExclusionWrapper(
        TablePartitionNamePatternWrapper included,
        SimpleNamePatternWrapper excluded) {
      super(included, excluded);
    }

    boolean hasTable(String tableName) {
      return has(tableName);
    }

    boolean hasFullTable(String tableName) {
      if (mExcluded.isEmpty()) {
        return mIncluded.hasFullTable(tableName);
      } else {
        // tables that are implicitly included by being NOT excluded are meant to be fully bypassed
        // since there is no way to specify partitions in an exclusion list
        return !mExcluded.has(tableName);
      }
    }

    boolean hasPartition(String tableName, String partName) {
      if (mExcluded.isEmpty()) {
        return mIncluded.hasPartition(tableName, partName);
      } else {
        return !mExcluded.has(tableName);
      }
    }
  }

  /**
   * Type alias for {@link InclusionExclusionWrapper<SimpleNamePatternWrapper>}.
   */
  static final class NamePatternInclusionExclusionWrapper
      extends InclusionExclusionWrapper<SimpleNamePatternWrapper> {
    NamePatternInclusionExclusionWrapper(SimpleNamePatternWrapper included,
                                         SimpleNamePatternWrapper excluded) {
      super(included, excluded);
    }
  }

  /**
   * Wrapper for table and partition specs, only appears in bypass-include list.
   */
  static class TablePartitionNamePatternWrapper extends NamePatternWrapper {
    private final ImmutableMap<String, Set<NamePatternInclusionExclusionWrapper>> mTablePartMap;

    TablePartitionNamePatternWrapper(
        Set<String> fullTableNames,
        Set<Pattern> patterns,
        Map<String, Set<NamePatternInclusionExclusionWrapper>> tablePartMap) {
      super(fullTableNames, patterns);
      mTablePartMap = ImmutableMap.copyOf(tablePartMap);
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
      Set<NamePatternInclusionExclusionWrapper> wrappers = mTablePartMap.get(tableName);
      if (wrappers == null) {
        // no partition specs, so it's not bypassed at all
        return false;
      }
      // it's partially bypassed
      // here we know for sure `wrappers.size() != 0`
      return wrappers.stream().anyMatch(p -> p.has(partName));
    }
  }

  /**
   * Type alias for NamePatternWrapper.
   */
  static final class SimpleNamePatternWrapper extends NamePatternWrapper {
    SimpleNamePatternWrapper(Set<String> names, Set<Pattern> patterns) {
      super(names, patterns);
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
   * @param <SELFT> the subtype of deriving class
   */
  public abstract static class BaseBuilder<T, SELFT extends BaseBuilder<T, SELFT>> {
    @Nullable
    protected Set<String> mNames;
    @Nullable
    protected Set<Pattern> mPatterns;

    protected BaseBuilder() {}

    /**
     * Returns this instance.
     * @return this instance
     */
    abstract SELFT self();

    /**
     * Adds a name.
     * @param name name
     * @return the builder
     */
    public SELFT addName(String name) {
      mNames = addOrNewSet(mNames, name);
      return self();
    }

    /**
     * Adds a pattern.
     * @param pattern pattern
     * @return the builder
     */
    public SELFT addPattern(Pattern pattern) {
      mPatterns = addOrNewSet(mPatterns, pattern);
      return self();
    }

    /**
     * Adds a set of names, the set has to be mutable.
     * @param names set of names
     * @return the builder
     */
    public SELFT addNames(Set<String> names) {
      mNames = addOrNewSet(mNames, names);
      return self();
    }

    /**
     * Adds a set of patterns, the set has to be mutable.
     * @param patterns set of names
     * @return the builder
     */
    public SELFT addPatterns(Set<Pattern> patterns) {
      mPatterns = addOrNewSet(mPatterns, patterns);
      return self();
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
  abstract static class IncludeExcludeBuilder<I, IBUILDERT extends BaseBuilder<I, IBUILDERT>,
                                              E, EBUILDERT extends BaseBuilder<E, EBUILDERT>,
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
  public static final class SimpleNamePatternWrapperBuilder
      extends BaseBuilder<SimpleNamePatternWrapper, SimpleNamePatternWrapperBuilder> {
    @Override
    SimpleNamePatternWrapperBuilder self() {
      return this;
    }

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
  public static final class TablePartitionNamePatternWrapperBuilder
      extends BaseBuilder<TablePartitionNamePatternWrapper,
      TablePartitionNamePatternWrapperBuilder> {
    private Map<String, Set<NamePatternInclusionExclusionWrapper>> mTablePartMap;

    @Override
    TablePartitionNamePatternWrapperBuilder self() {
      return this;
    }

    /**
     * Attaches partition specification to a table.
     * @param tableName the table name
     * @param builder builder for partition specification
     * @return the builder
     */
    public TablePartitionNamePatternWrapperBuilder addPartition(String tableName,
                                                                PartitionSpecBuilder builder) {
      return addPartition(tableName, builder.build());
    }

    TablePartitionNamePatternWrapperBuilder addPartition(
        String tableName,
        NamePatternInclusionExclusionWrapper partition) {
      if (mTablePartMap == null) {
        mTablePartMap = new HashMap<>();
      }
      Set<NamePatternInclusionExclusionWrapper> set = mTablePartMap.get(tableName);
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
      TablePartitionInclusionExclusionWrapper> {

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
    TablePartitionInclusionExclusionWrapper build() {
      return new TablePartitionInclusionExclusionWrapper(include().build(), exclude().build());
    }
  }

  /**
   * Builder for the ignore entry.
   */
  static class NamePatternInclusionExclusionWrapperBuilder extends IncludeExcludeBuilder<
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      SimpleNamePatternWrapper,
      SimpleNamePatternWrapperBuilder,
      NamePatternInclusionExclusionWrapper> {

    protected NamePatternInclusionExclusionWrapperBuilder(
        SimpleNamePatternWrapperBuilder includeBuilder,
        SimpleNamePatternWrapperBuilder excludeBuilder) {
      super(includeBuilder, excludeBuilder);
    }

    /**
     * Creates a new builder.
     */
    public NamePatternInclusionExclusionWrapperBuilder() {
      this(new SimpleNamePatternWrapperBuilder(), new SimpleNamePatternWrapperBuilder());
    }

    @Override
    public NamePatternInclusionExclusionWrapper build() {
      return new NamePatternInclusionExclusionWrapper(include().build(), exclude().build());
    }
  }

  /**
   * Type alias for {@link NamePatternInclusionExclusionWrapperBuilder} \
   * when it is used to build a partition spec.
   */
  public static class PartitionSpecBuilder extends NamePatternInclusionExclusionWrapperBuilder {}

  /**
   * Type alias for {@link NamePatternInclusionExclusionWrapperBuilder} \
   * when it is used to build an ignore entry.
   */
  public static class IgnoreEntryBuilder extends NamePatternInclusionExclusionWrapperBuilder {}

  /**
   * Builder for {@link UdbAttachSpec}.
   */
  public static class Builder {
    private final BypassEntryBuilder mBypassBuilder;
    private final IgnoreEntryBuilder mIgnoreBuilder;

    /**
     * Creates a new builder.
     */
    public Builder() {
      mBypassBuilder = new BypassEntryBuilder();
      mIgnoreBuilder = new IgnoreEntryBuilder();
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
    public IgnoreEntryBuilder ignore() {
      return mIgnoreBuilder;
    }

    /**
     * Builds the spec.
     * @return the builder
     */
    public UdbAttachSpec build() {
      TablePartitionInclusionExclusionWrapper bypassed = mBypassBuilder.build();
      NamePatternInclusionExclusionWrapper ignored = mIgnoreBuilder.build();
      return new UdbAttachSpec(bypassed, ignored);
    }
  }
}
