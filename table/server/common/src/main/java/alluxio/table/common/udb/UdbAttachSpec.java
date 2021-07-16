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
   * @see UdbAttachSpec#isFullyBypassedTable(String)
   */
  public boolean isBypassedTable(String tableName) {
    return shadowedByIgnore(tableName, mBypassed.containsTable(tableName));
  }

  /**
   * Checks if all partitions of a table should be bypassed.
   *
   * @param tableName the table name
   * @return true if the table is configured to be fully bypassed, false otherwise
   * @see UdbAttachSpec#isBypassedTable(String)
   */
  public boolean isFullyBypassedTable(String tableName) {
    return shadowedByIgnore(tableName, mBypassed.containsFullTable(tableName));
  }

  /**
   * Checks if a table is ignored.
   *
   * @param tableName the table name
   * @return true if the table is ignored, false otherwise
   */
  public boolean isIgnoredTable(String tableName) {
    return mIgnored.contains(tableName);
  }

  /**
   * Checks by a partition's name if it should be bypassed.
   *
   * @param tableName the table name
   * @param partitionName the partition name
   * @return true if the partition should be bypassed, false otherwise
   */
  public boolean isBypassedPartition(String tableName, String partitionName) {
    return shadowedByIgnore(tableName, mBypassed.containsPartition(tableName, partitionName));
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

  /**
   * Wrapper class that takes into account inclusion and exclusion behaviour.
   */
  abstract static class InclusionExclusionWrapper<T extends NamePatternWrapper> {
    // T can be TablePartitionNamePatternWrapper when used in a bypass spec,
    // or SimpleNamePatternWrapper when used in an ignore spec, or a partition spec
    protected final T mIncluded;
    // excluded entries don't allow partition specifications -- they are name and regex only
    // therefore type parameter T is not used for the exclusion list
    protected final SimpleNamePatternWrapper mExcluded;

    InclusionExclusionWrapper(T included, SimpleNamePatternWrapper excluded) {
      // currently either included or excluded list is accepted, but not both
      Preconditions.checkArgument(included.isEmpty() || excluded.isEmpty());
      mIncluded = included;
      mExcluded = excluded;
    }

    /**
     * Checks if a name is contained in this wrapper.
     * The generic implementation here follows this logic:
     * 1. when the exclusion list is empty, check if the name is contained in the inclusion list;
     * 2. when the exclusion list is not empty, check if the name is not excluded by the exclusion
     *    list.
     * This logic has this implication: when both lists are empty, nothing is contained in this
     * wrapper (as opposed to the expectation that an empty exclusion list contains everything).
     * Deriving classes may override the default implementation, but should be careful to stay
     * consistent with the generic behaviour, so as to avoid any surprises.
     *
     * @param name the name to check
     * @return true if the name is contained in this wrapper, false otherwise
     */
    boolean contains(String name) {
      if (mExcluded.isEmpty()) {
        return mIncluded.contains(name);
      } else {
        return !mExcluded.contains(name);
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

    boolean containsTable(String tableName) {
      return contains(tableName);
    }

    boolean containsFullTable(String tableName) {
      if (mExcluded.isEmpty()) {
        return mIncluded.containsFullTable(tableName);
      } else {
        // tables that are implicitly included by being NOT excluded are meant to be fully bypassed
        // since there is no way to specify partitions in an exclusion list
        return !mExcluded.contains(tableName);
      }
    }

    boolean containsPartition(String tableName, String partName) {
      if (mExcluded.isEmpty()) {
        return mIncluded.containsPartition(tableName, partName);
      } else {
        return !mExcluded.contains(tableName);
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

    /**
     * A table is considered a "full" table, if it meets one of the following conditions in order:
     * 1. its name is explicitly specified as a string literal;
     * 2. it has an associated partition specification, which
     *    a) is empty, then it's a full table;
     *    b) is not empty, then it's not a full table;
     * 3. its name is captured by a pattern.
     * @param tableName
     * @return
     */
    boolean containsFullTable(String tableName) {
      // if a table is listed with an explicit literal name, then the partition specification
      // or regex patterns should not be consulted:
      // here use parent's hasExplicit since self's takes into account partition specs
      if (super.containsExplicit(tableName)) {
        return true;
      }
      if (mTablePartMap.containsKey(tableName)) {
        // otherwise, check if it has a partition specification.
        // an empty partition spec signals a full table, a non-empty one signals otherwise.
        // either way, it is explicitly listed, so no need to consult the regex patterns.
        return mTablePartMap.get(tableName).size() == 0;
      }
      // otherwise, check if it is covered by a regex pattern:
      return containsByPattern(tableName);
    }

    @Override
    boolean contains(String tableName) {
      return super.contains(tableName) || mTablePartMap.containsKey(tableName);
    }

    /**
     * This overrides the parent's behaviour by adding a new criterion for what counts as an
     * explicitly specified table: if it has a partition specification.
     * @param tableName
     * @return
     */
    @Override
    boolean containsExplicit(String tableName) {
      return super.containsExplicit(tableName) || mTablePartMap.containsKey(tableName);
    }

    /**
     * Alias of {@link TablePartitionNamePatternWrapper#contains(String)}.
     */
    boolean containsTable(String tableName) {
      return contains(tableName);
    }

    /**
     * A partition is considered contained in this wrapper, if it meets the following conditions:
     * 1. the parent table is a full table (see {@link #containsFullTable(String)};
     * 2. the parent table has partition specifications, and the partition is contained in one
     *    of the specifications.
     * @param tableName
     * @param partName
     * @return
     */
    boolean containsPartition(String tableName, String partName) {
      // any partition of a fully bypassed table is bypassed
      if (containsFullTable(tableName)) {
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
      return wrappers.stream().anyMatch(p -> p.contains(partName));
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

    boolean contains(String name) {
      return containsExplicit(name) || containsByPattern(name);
    }

    boolean containsExplicit(String name) {
      return mNames.contains(name);
    }

    boolean containsByPattern(String name) {
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
      // make sure no tables are added both as a simple name and with partition specs
      // at the same time.
      Set<String> intersection = Sets.intersection(mNames, mTablePartMap.keySet());
      Preconditions.checkArgument(
          intersection.isEmpty(),
          "Tables ({}) should be specified either as a simple name, "
          + "or with partition specs, but not both",
          String.join(", ", intersection)
      );
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
