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

package alluxio.master.table;

import alluxio.table.common.udb.UdbAttachOptions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.annotation.Nullable;

/**
 * The Alluxio db config file.
 *
 * Syntax specification:
 * 1. Top level object:
 *    DbConfig :=
 *        {"bypass": BypassTablesSpecObject}
 *        | {"ignore": IgnoreTablesSpecObject}
 *        | {"bypass": BypassTablesSpecObject, "ignore": IgnoreTablesSpecObject}
 *    DbConfig is the outermost object of the json config file. Currently allows
 *    configurations for bypassing and ignoring tables and partitions.
 *
 * 2. Second level objects:
 *    i.   BypassTablesSpecObject :=
 *             {"tables": BypassTablesIncludeExcludeObject}
 *    ii.  IgnoreTablesSpecObject :=
 *             {"tables": SimpleIncludeExcludeObject}
 *
 * 3. Inclusion/exclusion lists and objects:
 *    i.   SimpleNameRegexList := [ (NameObject | RegexObject)* ]
 *         SimpleNameRegexList is an mixed array of name and regex objects.
 *    ii.  BypassIncludeTablePartitionList :=
 *             [ (NameObject | RegexObject | BypassTablePartitionSpecObject)* ]
 *         BypassIncludeTablePartitionList is a superset of SimpleNameRegexList, with the additional
 *         capability of containing BypassTablePartitionSpecObjects.
 *    iv.  SimpleIncludeExcludeObject :=
 *             {"include": SimpleNameRegexList} | {"exclude": SimpleNameRegexList}
 *         SimpleIncludeExcludeObject is an enum of either an inclusion list or an exclusion
 *         list. Names and regexes can be specified in the lists.
 *    iii. BypassTablesIncludeExcludeObject :=
 *             {"include": BypassIncludeTablePartitionList} | {"exclude": SimpleNameRegexList}
 *         BypassTablesIncludeExcludeObject is a superset of SimpleIncludeExcludeObject.
 *         In case of an inclusion list, further partition specifications are allowed, while
 *         in case of an exclusion list, only names and regexes are allowed.
 *
 * 4. Table/partition specification object:
 *    i.   BypassTablePartitionSpecObject :=
 *           {
 *             "type": "partition_spec",
 *             "table": NameLiteral,
 *             "partitions": SimpleIncludeExcludeObject
 *           }
 *       BypassTablePartitionSpecObject has a `table` field that contains the table name for which
 *       the partition specification is bound to. The `partitions` field follows the same
 *       convention that allows either an array of included items, or an object that allows
 *       to explicitly specify inclusions and exclusions.
 *    ii.  NameObject := {"type": "name", "name": NameLiteral}
 *         A string literal for table and partition names.
 *    iii. RegexObject := {"type":"regex", "regex": RegexLiteral}
 *         An object with a `regex` field that contains a regex literal.
 *    iv.  RegexLiteral
 *         A string representation of a regular expression.
 *    v.   NameLiteral
 *         An exact name for a table or partition.
 *
 * An example:
 * {                  <- DbConfig
 *   "bypass": {        <- BypassTablesSpecObject
 *     "tables": {        <- BypassTablesIncludeExcludeObject
 *       "include": [       <- BypassIncludeTablePartitionList
 *         {                  <- NameObject
 *           "type": "name",
 *           "name": "table1",   <- NameLiteral
 *         },
 *         {                         <- BypassTablePartitionSpecObject
 *           "type": "partition_spec",
 *           "table": "table2",
 *           "partitions": {         <- SimpleIncludeExcludeObject
 *             "exclude": [           <- SimpleNameRegexList
 *               {"type": "name", "name": "part1"},
 *               {                        <- RegexObject
 *                 "type": "regex",
 *                 "regex": "part\\d\\d"    <-RegexLiteral
 *               }
 *             ]
 *           }
 *         }
 *       ]
 *     }
 *   },
 *   "ignore": {        <- IgnoreTablesSpecObject
 *     "tables": {
 *       "include": [
 *         {"type": "name", "name": "table4"}
 *     }
 *   }
 * }
 */
public final class DbConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DbConfig.class);

  static final String FIELD_BYPASS = "bypass";
  static final String FIELD_IGNORE = "ignore";

  private final BypassTablesSpec mBypassEntry;
  private final IgnoreTablesSpec mIgnoreEntry;

  private static final DbConfig EMPTY_INSTANCE = new DbConfig(null, null);

  /**
   * Json creator.
   *
   * @param bypassEntry bypass entry
   * @param ignoreEntry ignore entry
   */
  @JsonCreator
  public DbConfig(@JsonProperty(FIELD_BYPASS) @Nullable BypassTablesSpec bypassEntry,
                  @JsonProperty(FIELD_IGNORE) @Nullable IgnoreTablesSpec ignoreEntry) {
    mBypassEntry = bypassEntry == null ? new BypassTablesSpec(null) : bypassEntry;
    mIgnoreEntry = ignoreEntry == null ? new IgnoreTablesSpec(null) : ignoreEntry;
  }

  /**
   * Returns an empty configuration.
   *
   * @return an empty config instance
   */
  public static DbConfig empty() {
    return EMPTY_INSTANCE;
  }

  /**
   * Converts to a {@link UdbAttachOptions}.
   *
   * @return the {@link UdbAttachOptions} object
   */
  public UdbAttachOptions getUdbAttachOptions() {
    UdbAttachOptions.Builder builder = new UdbAttachOptions.Builder();
    // process included bypassed tables
    builder.bypass().include();
    for (TablePartitionSpecObject entry : mBypassEntry.getTables().getIncludedEntries()) {
      // entry can be a simple name, a pattern, or a table name with partition specifications
      switch (entry.getType()) {
        case NAME:
          builder.addTable(((NameObject) entry).getName());
          continue;
        case REGEX:
          builder.addTable(((RegexObject) entry).getPattern());
          continue;
        default:
      }
      // otherwise, process partition specifications
      PartitionSpecObject casted = (PartitionSpecObject) entry;
      IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> list = casted.getPartitions();
      // process included and excluded partitions
      Set<NameOrRegexObject> partitions;
      if (list.hasIncludedEntries()) {
        builder.include();
        partitions = list.getIncludedEntries();
      } else {
        builder.exclude();
        partitions = list.getExcludedEntries();
      }
      for (NameOrRegexObject partition : partitions) {
        switch (partition.getType()) {
          case NAME:
            builder.addPartition(casted.getTableName(), ((NameObject) partition).getName());
            break;
          case REGEX:
            builder.addPartition(casted.getTableName(), ((RegexObject) partition).getPattern());
            break;
          default:
        }
      }
    }
    // process excluded bypassed tables
    builder.bypass().exclude();
    addTables(builder, mBypassEntry.getTables().getExcludedEntries());
    // process ignored tables
    builder.ignore();
    Set<NameOrRegexObject> tables;
    if (mIgnoreEntry.getTables().hasIncludedEntries()) {
      builder.include();
      tables = mIgnoreEntry.getTables().getIncludedEntries();
    } else {
      builder.exclude();
      tables = mIgnoreEntry.getTables().getExcludedEntries();
    }
    addTables(builder, tables);

    return builder.build();
  }

  private static void addTables(UdbAttachOptions.Builder builder, Set<NameOrRegexObject> tables) {
    for (NameOrRegexObject entry : tables) {
      switch (entry.getType()) {
        case NAME:
          builder.addTable(((NameObject) entry).getName());
          break;
        case REGEX:
          builder.addTable(((RegexObject) entry).getPattern());
          break;
        default:
      }
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (!(other instanceof DbConfig)) {
      return false;
    }
    DbConfig that = (DbConfig) other;
    return Objects.equals(mBypassEntry, that.mBypassEntry)
        && Objects.equals(mIgnoreEntry, that.mIgnoreEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mBypassEntry, mIgnoreEntry);
  }

  /**
   * Type alias for TablesEntry<TablePartitionSpecObject, NameOrRegexObject>.
   */
  static final class BypassTablesSpec
      extends TablesEntry<TablePartitionSpecObject, NameOrRegexObject> {
    @JsonCreator
    BypassTablesSpec(@JsonProperty(FIELD_TABLES) @Nullable
                         IncludeExcludeObject<TablePartitionSpecObject, NameOrRegexObject> list) {
      super(list);
    }
  }

  /**
   * Type alias for TablesEntry<NameOrRegexObject, NameOrRegexObject>.
   */
  static final class IgnoreTablesSpec extends TablesEntry<NameOrRegexObject, NameOrRegexObject> {
    @JsonCreator
    IgnoreTablesSpec(@JsonProperty(FIELD_TABLES) @Nullable
                         IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> list) {
      super(list);
    }
  }

  /**
   * The "tables" object: {"tables": ... }.
   * Base class for BypassTablesSpec and IgnoreTablesSpec.
   * @param <IncludeT> the type of included entries of IncludeExcludeObject
   * @param <ExcludeT> the type of excluded entries of ExcludeExcludeObject
   */
  static class TablesEntry<IncludeT extends TablePartitionSpecObject,
                           ExcludeT extends TablePartitionSpecObject> {
    static final String FIELD_TABLES = "tables";

    private final IncludeExcludeObject<IncludeT, ExcludeT> mTables;

    @JsonCreator
    protected TablesEntry(
        @JsonProperty(FIELD_TABLES) @Nullable IncludeExcludeObject<IncludeT, ExcludeT> list) {
      mTables = list == null ? IncludeExcludeObject.empty() : list;
    }

    IncludeExcludeObject<IncludeT, ExcludeT> getTables() {
      return mTables;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (this == other) {
        return true;
      }
      if (!(other instanceof TablesEntry)) {
        return false;
      }
      TablesEntry that = (TablesEntry) other;
      return Objects.equals(mTables, that.mTables);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mTables);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add(FIELD_TABLES, mTables)
          .toString();
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = TablePartitionSpecObject.FIELD_TYPE)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = NameObject.class, name = TablePartitionSpecObject.TYPE_NAME),
      @JsonSubTypes.Type(value = RegexObject.class, name = TablePartitionSpecObject.TYPE_REGEX),
      @JsonSubTypes.Type(value = PartitionSpecObject.class,
                         name = TablePartitionSpecObject.TYPE_PARTITION_SPEC)})
  interface TablePartitionSpecObject {
    String FIELD_TYPE = "type";
    String TYPE_NAME = "name";
    String TYPE_REGEX = "regex";
    String TYPE_PARTITION_SPEC = "partition_spec";

    enum Type {
      NAME(TYPE_NAME), REGEX(TYPE_REGEX), PARTITION_SPEC(TYPE_PARTITION_SPEC);

      private final String mType;

      Type(String type) {
        mType = type;
      }

      @Override
      public String toString() {
        return mType;
      }
    }

    Type getType();
  }

  /**
   * Tag interface that's implemented for {@link NameObject} and {@link RegexObject}.
   */
  interface NameOrRegexObject extends TablePartitionSpecObject {}

  abstract static class AbstractSpecObject implements TablePartitionSpecObject {
    protected final Type mType;

    protected AbstractSpecObject(Type type) {
      mType = type;
    }

    public Type getType() {
      return mType;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (this == other) {
        return true;
      }
      if (!(other instanceof AbstractSpecObject)) {
        return false;
      }
      AbstractSpecObject casted = (AbstractSpecObject) other;
      return getType() == casted.getType();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mType);
    }
  }

  static class NameObject extends AbstractSpecObject implements NameOrRegexObject {
    static final String FIELD_NAME = "name";
    private final String mName;

    @JsonCreator
    public NameObject(@JsonProperty(FIELD_NAME) String name) {
      super(Type.NAME);
      Preconditions.checkArgument(name != null, "Name is null");
      Preconditions.checkArgument(!name.isEmpty(), "Empty name");
      mName = name;
    }

    public String getName() {
      return mName;
    }

    @Override
    public boolean equals(Object other) {
      if (!super.equals(other)) {
        return false;
      }
      if (!(other instanceof NameObject)) {
        return false;
      }
      NameObject casted = (NameObject) other;
      return Objects.equals(mName, casted.mName);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + Objects.hashCode(mName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add(FIELD_TYPE, mType)
          .add(FIELD_NAME, mName)
          .toString();
    }
  }

  static class RegexObject extends AbstractSpecObject implements NameOrRegexObject {
    static final String FIELD_REGEX = "regex";
    private final Pattern mPattern;

    @JsonCreator
    public RegexObject(@JsonProperty(FIELD_REGEX) String pattern) {
      super(Type.REGEX);
      Preconditions.checkArgument(pattern != null, "Pattern is null");
      try {
        mPattern = Pattern.compile(pattern);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid regex syntax", e);
      }
    }

    public Pattern getPattern() {
      return mPattern;
    }

    @Override
    public boolean equals(Object other) {
      if (!super.equals(other)) {
        return false;
      }
      if (!(other instanceof RegexObject)) {
        return false;
      }
      RegexObject casted = (RegexObject) other;
      return Objects.equals(mPattern.pattern(), casted.mPattern.pattern());
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + Objects.hashCode(mPattern.pattern());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add(FIELD_TYPE, mType)
          .add(FIELD_REGEX, mPattern.pattern())
          .toString();
    }
  }

  static class PartitionSpecObject extends AbstractSpecObject {
    static final String FIELD_TABLE = "table";
    static final String FIELD_PARTITIONS = "partitions";
    private final String mTableName;
    private final IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> mPartitions;

    @JsonCreator
    public PartitionSpecObject(
        @JsonProperty(FIELD_TABLE) String tableName,
        @JsonProperty(FIELD_PARTITIONS)
            IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> partitions
    ) {
      super(Type.PARTITION_SPEC);
      Preconditions.checkArgument(tableName != null, "Table name is null");
      Preconditions.checkArgument(partitions != null, "Partitions is null");
      mTableName = tableName;
      mPartitions = partitions;
    }

    public String getTableName() {
      return mTableName;
    }

    public IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> getPartitions() {
      return mPartitions;
    }

    @Override
    public boolean equals(Object other) {
      if (!super.equals(other)) {
        return false;
      }
      if (!(other instanceof PartitionSpecObject)) {
        return false;
      }
      PartitionSpecObject casted = (PartitionSpecObject) other;
      // partitions are deliberately excluded in `equals` impl to avoid conflicts
      // when there's a NameObject and a PartitionSpecObject with the same table name
      // in the set of IncludeExcludeObject
      return Objects.equals(mTableName, casted.mTableName);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + Objects.hashCode(mTableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add(FIELD_TYPE, mType)
          .add(FIELD_TABLE, mTableName)
          .add(FIELD_PARTITIONS, mPartitions)
          .toString();
    }
  }

  static class IncludeExcludeObject<IncludeT extends TablePartitionSpecObject,
                                    ExcludeT extends TablePartitionSpecObject> {
    static final String FIELD_INCLUDE = "include";
    static final String FIELD_EXCLUDE = "exclude";
    private static final IncludeExcludeObject EMPTY_INSTANCE =
        new IncludeExcludeObject(null, null);

    private final Set<IncludeT> mIncludedEntries;
    private final Set<ExcludeT> mExcludedEntries;

    @JsonCreator
    public IncludeExcludeObject(@JsonProperty(FIELD_INCLUDE) @Nullable Set<IncludeT> included,
                                @JsonProperty(FIELD_EXCLUDE) @Nullable Set<ExcludeT> excluded) {
      mIncludedEntries = included == null ? Collections.emptySet() : included;
      mExcludedEntries = excluded == null ? Collections.emptySet() : excluded;
      // included and excluded cannot be non-empty at the same time
      Preconditions.checkArgument(
          mIncludedEntries.isEmpty() || mExcludedEntries.isEmpty(),
          "Either include or exclude can be specified, but not both"
      );
    }

    public static <IncludeT extends TablePartitionSpecObject,
                   ExcludeT extends TablePartitionSpecObject>
            IncludeExcludeObject<IncludeT, ExcludeT> empty() {
      return EMPTY_INSTANCE;
    }

    public boolean hasIncludedEntries() {
      return !mIncludedEntries.isEmpty();
    }

    public boolean hasExcludedEntries() {
      return !mExcludedEntries.isEmpty();
    }

    public Set<IncludeT> getIncludedEntries() {
      return mIncludedEntries;
    }

    public Set<ExcludeT> getExcludedEntries() {
      return mExcludedEntries;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (this == other) {
        return true;
      }
      if (!(other instanceof IncludeExcludeObject)) {
        return false;
      }
      IncludeExcludeObject casted = (IncludeExcludeObject) other;
      return Objects.equals(mIncludedEntries, casted.mIncludedEntries)
          && Objects.equals(mExcludedEntries, casted.mExcludedEntries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mIncludedEntries, mExcludedEntries);
    }

    @Override
    public String toString() {
      MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
      if (hasIncludedEntries()) {
        helper.add(FIELD_INCLUDE, mIncludedEntries);
      } else {
        helper.add(FIELD_EXCLUDE, mExcludedEntries);
      }
      return helper.toString();
    }
  }
}
