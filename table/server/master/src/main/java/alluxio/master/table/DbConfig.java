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

import alluxio.table.common.udb.UdbAttachSpec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 *             {"tables": BypassIncludeTablePartitionList | BypassTablesIncludeExcludeObject}
 *    ii.  IgnoreTablesSpecObject :=
 *             {"tables": SimpleNameRegexList | SimpleIncludeExcludeObject}
 *    Each of the `tables` fields accepts either one of the followings:
 *      a) an array (`BypassIncludeTablePartitionList` or `SimpleNameRegexList`)
 *      b) an object (`BypassTablesIncludeExcludeObject` or `SimpleIncludeExcludeObject`)
 *
 * 3. Inclusion/exclusion lists and objects:
 *    i.   SimpleNameRegexList := [ (NameLiteral | RegexObject)* ]
 *         SimpleNameRegexList is an mixed array of name literals and regex objects.
 *    ii.  BypassIncludeTablePartitionList :=
 *             [ (NameLiteral | RegexObject | BypassTablePartitionSpecObject)* ]
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
 * 4. Table partition specification object:
 *    i.   BypassTablePartitionSpecObject :=
 *           {
 *             "table": NameLiteral,
 *             "partitions": SimpleNameRegexList | SimpleIncludeExcludeObject
 *           }
 *       BypassTablePartitionSpecObject has a `table` field that contains the table name for which
 *       the partition specification is bound to. The `partitions` field follows the same
 *       convention that allows either an array of included items, or an object that allows
 *       to explicitly specify inclusions and exclusions.
 *
 * 5. Others:
 *    i.   NameLiteral
 *         A string literal for table and partition names.
 *    ii.  RegexLiteral
 *         A string representation of a regular expression.
 *    ii.  RegexObject := {"regex": RegexLiteral}
 *         An object with a `regex` field that contains a regex literal.
 *
 * An example:
 * {                  <- DbConfig
 *   "bypass": {        <- BypassTablesSpecObject
 *     "tables": {        <- BypassTablesIncludeExcludeObject
 *       "include": [       <- BypassIncludeTablePartitionList
 *         "table1",          <- NameLiteral
 *         {                    <- BypassTablePartitionSpecObject
 *           "table": "table2",
 *           "partitions": {         <- SimpleIncludeExcludeObject
 *             "exclude": [           <- SimpleNameRegexList
 *               "part1",
 *               {                        <- RegexObject
 *                 "regex": "part\\d\\d"    <-RegexLiteral
 *               }
 *             ]
 *           }
 *         }
 *       ]
 *     }
 *   },
 *   "ignore": {        <- IgnoreTablesSpecObject
 *     "tables": ["table4"]  <- SimpleNameRegexList
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
   * Converts to a {@link UdbAttachSpec}.
   *
   * @return the {@link UdbAttachSpec} object
   */
  public UdbAttachSpec getUdbAttachSpec() {
    UdbAttachSpec.Builder builder = new UdbAttachSpec.Builder();
    // process included bypassed tables
    for (TableEntry entry : mBypassEntry.getList().getIncludedEntries()) {
      // entry can be a simple name, a pattern, or a table name with partition specifications
      if (entry.isPattern()) {
        // adds it as a pattern and we are done
        builder.bypass().include().addPattern(entry.getPattern());
        continue;
      }
      if (entry.getPartitions().isEmpty()) {
        // no partition specifications, add it as a simple name, and we are done
        builder.bypass().include().addName(entry.getName());
        continue;
      }
      // otherwise, process partition specifications
      IncludeExcludeList<NamePatternEntry> partitions = entry.getPartitions();
      UdbAttachSpec.PartitionSpecBuilder partitionBuilder =
          new UdbAttachSpec.PartitionSpecBuilder();
      // process included and excluded partitions
      for (NamePatternEntry partition : partitions.getIncludedEntries()) {
        if (partition.isPattern()) {
          partitionBuilder.include().addPattern(partition.getPattern());
        } else {
          partitionBuilder.include().addName(partition.getName());
        }
      }
      for (NamePatternEntry partition : partitions.getExcludedEntries()) {
        if (partition.isPattern()) {
          partitionBuilder.exclude().addPattern(partition.getPattern());
        } else {
          partitionBuilder.exclude().addName(partition.getName());
        }
      }
      // finally, add the partition spec with the table name
      builder.bypass().include().addPartition(entry.getTable(), partitionBuilder);
    }

    // process excluded bypassed tables
    for (NamePatternEntry entry : mBypassEntry.getList().getExcludedEntries()) {
      if (entry.isPattern()) {
        builder.bypass().exclude().addPattern(entry.getPattern());
      } else {
        builder.bypass().exclude().addName(entry.getName());
      }
    }

    // process included ignored tables
    for (NamePatternEntry entry : mIgnoreEntry.getList().getIncludedEntries()) {
      if (entry.isPattern()) {
        builder.ignore().include().addPattern(entry.getPattern());
      } else {
        builder.ignore().include().addName(entry.getName());
      }
    }

    // process excluded ignored tables
    for (NamePatternEntry entry : mIgnoreEntry.getList().getExcludedEntries()) {
      if (entry.isPattern()) {
        builder.ignore().exclude().addPattern(entry.getPattern());
      } else {
        builder.ignore().exclude().addName(entry.getName());
      }
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object other) {
    if (!checkReferenceAndClassEquality(this, other)) {
      return false;
    }
    DbConfig that = (DbConfig) other;
    return Objects.equals(mBypassEntry, that.mBypassEntry)
        && Objects.equals(mIgnoreEntry, that.mIgnoreEntry);
  }

  private static boolean checkReferenceAndClassEquality(Object self, Object other) {
    if (self == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    // we dont compare by instanceof because we dont want to compare
    // a subclass instance with a superclass instance.
    return self.getClass() == other.getClass();
  }

  @Override
  public int hashCode() {
    return Objects.hash(mBypassEntry, mIgnoreEntry);
  }

  /**
   * Type alias for TablesEntry<TableEntry>.
   */
  @JsonDeserialize(using = BypassTablesSpecDeserializer.class)
  static final class BypassTablesSpec extends TablesEntry<TableEntry> {
    // inherited: IncludeExcludeList<TableEntry>
    // this is the BypassTablesObject from syntax specification

    BypassTablesSpec(@Nullable IncludeExcludeList<TableEntry> list) {
      super(list);
    }
  }

  /**
   * Type alias for TablesEntry<NamePatternEntry>.
   */
  @JsonDeserialize(using = IgnoreTablesSpecDeserializer.class)
  static final class IgnoreTablesSpec extends TablesEntry<NamePatternEntry> {
    // inherited: IncludeExcludeList<NamePatternEntry>
    // this is the SimpleIncludeExcludeObject from syntax specification

    IgnoreTablesSpec(@Nullable IncludeExcludeList<NamePatternEntry> list) {
      super(list);
    }
  }

  /**
   * The "tables" object: {"tables": ... }.
   * Base class for BypassTablesSpec and IgnoreTablesSpec.
   * @param <T> the type of entry contained
   */
  static class TablesEntry<T extends NamePatternEntry> {
    static final String FIELD_TABLES = "tables";

    private final IncludeExcludeList<T> mList;

    protected TablesEntry(@Nullable IncludeExcludeList<T> list) {
      mList = list == null ? IncludeExcludeList.empty() : list;
    }

    IncludeExcludeList<T> getList() {
      return mList;
    }

    @Override
    public boolean equals(Object other) {
      if (!checkReferenceAndClassEquality(this, other)) {
        return false;
      }
      TablesEntry that = (TablesEntry) other;
      return Objects.equals(mList, that.mList);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mList);
    }
  }

  /**
   * Deserialize to an IncludeExcludeList of given inner type T.
   * When the contained object is an array, an include list is implied.
   * @param <T> the contained inner entry type
   */
  static class TablesEntryDeserializer<T extends NamePatternEntry> {
    IncludeExcludeList<T> deserializeToList(
        Class<T> type, JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      if (node == null) {
        return null;
      }
      if (!node.hasNonNull(TablesEntry.FIELD_TABLES)) {
        throw new JsonParseException(
            mapper.treeAsTokens(node),
            String.format("field `%s` missing or is null", TablesEntry.FIELD_TABLES)
        );
      }
      node = node.get(TablesEntry.FIELD_TABLES);
      IncludeExcludeListDeserializer<T> deserializer = new IncludeExcludeListDeserializer<>();
      return deserializer.deserialize(type, mapper.treeAsTokens(node), cxt);
    }
  }

  static final class IgnoreTablesSpecDeserializer extends JsonDeserializer<IgnoreTablesSpec> {
    @Override
    public IgnoreTablesSpec deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      TablesEntryDeserializer<NamePatternEntry> deserializer = new TablesEntryDeserializer<>();
      IncludeExcludeList<NamePatternEntry> list =
          deserializer.deserializeToList(NamePatternEntry.class, jp, cxt);
      return new IgnoreTablesSpec(list);
    }
  }

  static final class BypassTablesSpecDeserializer extends JsonDeserializer<BypassTablesSpec> {
    @Override
    public BypassTablesSpec deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      TablesEntryDeserializer<TableEntry> deserializer = new TablesEntryDeserializer<>();
      IncludeExcludeList<TableEntry> list =
          deserializer.deserializeToList(TableEntry.class, jp, cxt);
      return new BypassTablesSpec(list);
    }
  }

  /**
   * On top of a regular NamePatternEntry, contains additional partition specification.
   */
  @JsonDeserialize(using = TableEntryDeserializer.class)
  static class TableEntry extends NamePatternEntry {
    static final String FIELD_TABLE = "table";
    static final String FIELD_PARTITIONS = "partitions";

    private final IncludeExcludeList<NamePatternEntry> mPartitions;

    /**
     * Creates an instance with a specific table name and no partition specification.
     *
     * @param tableName table name
     */
    TableEntry(String tableName) {
      this(tableName, IncludeExcludeList.empty());
    }

    /**
     * Creates an instance with a specific table name and possibly partitions specifications.
     *
     * @param tableName table name
     * @param partitions partitions
     */
    TableEntry(String tableName, IncludeExcludeList<NamePatternEntry> partitions) {
      super(tableName);
      mPartitions = partitions;
    }

    /**
     * Creates an instance from a {@link NamePatternEntry} with no partition specification.
     * @param namePatternEntry name entry
     */
    TableEntry(NamePatternEntry namePatternEntry) {
      super(namePatternEntry);
      mPartitions = IncludeExcludeList.empty();
    }

    IncludeExcludeList<NamePatternEntry> getPartitions() {
      return mPartitions;
    }

    /**
     * Returns table name if the entry is not a regex entry.
     * @return table name, null if the entry is a regex entry
     */
    @Nullable
    String getTable() {
      return getName();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  // Accepts a simple table name, an regular expression, or
  // an object of form: {"table": "tableName", "partitions": ["part1", "part2"]}
  static final class TableEntryDeserializer extends JsonDeserializer<TableEntry> {
    @Override
    public TableEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      // try deserialize as a `NamePatternEntry` object first
      try {
        NamePatternEntryDeserializer deserializer = new NamePatternEntryDeserializer();
        NamePatternEntry namePatternEntry =
            deserializer.deserialize(mapper.treeAsTokens(node), cxt);
        return new TableEntry(namePatternEntry);
      } catch (JsonProcessingException e) {
        // ignore, and try deserialize as a `TableEntry` object
      }
      if (node == null) {
        return null;
      }
      // a BypassTablePartitionSpec object
      if (!node.hasNonNull(TableEntry.FIELD_TABLE)) {
        throw new JsonParseException(
            mapper.treeAsTokens(node),
            String.format("invalid syntax, expecting table name, regex, "
                + "or an object with a `%s` field", TableEntry.FIELD_TABLE)
        );
      }
      String tableName = node.get(TableEntry.FIELD_TABLE).asText();
      JsonNode partitionsList = node.get(TableEntry.FIELD_PARTITIONS);
      if (partitionsList == null) {
        LOG.warn("Partition specification is not found, use literal table name instead: {}",
            node);
        return new TableEntry(tableName);
      }
      IncludeExcludeListDeserializer<NamePatternEntry> deserializer =
          new IncludeExcludeListDeserializer<>();
      IncludeExcludeList<NamePatternEntry> partitions = deserializer.deserialize(
          NamePatternEntry.class, mapper.treeAsTokens(partitionsList), cxt);
      if (partitions == null) {
        partitions = IncludeExcludeList.empty();
      }
      return new TableEntry(tableName, partitions);
    }
  }

  /**
   * Wrapper of a explicit name or a regular expression.
   */
  @JsonDeserialize(using = NamePatternEntryDeserializer.class)
  static class NamePatternEntry {
    static final String FIELD_REGEX = "regex";

    private final boolean mIsPattern;
    private final Pattern mPattern;
    private final String mName;

    /**
     * Creates an entry with a simple name.
     *
     * @param name table or partition name
     */
    NamePatternEntry(String name) {
      Preconditions.checkArgument(!name.isEmpty(), "empty name");
      mIsPattern = false;
      mPattern = null;
      mName = name;
    }

    /**
     * Creates an instance with a regex.
     *
     * @param regex regex
     */
    NamePatternEntry(Pattern regex) {
      mIsPattern = true;
      mPattern = regex;
      mName = null;
    }

    /**
     * Copy constructor.
     *
     * @param namePatternEntry instance to copy
     */
    NamePatternEntry(NamePatternEntry namePatternEntry) {
      mIsPattern = namePatternEntry.mIsPattern;
      mName = namePatternEntry.mName;
      mPattern = namePatternEntry.mPattern;
    }

    /**
     * @return if the entry contains a pattern
     */
    boolean isPattern() {
      return mIsPattern;
    }

    /**
     * @return pattern if the entry contains a pattern, null otherwise
     */
    @Nullable
    Pattern getPattern() {
      return mPattern;
    }

    /**
     * @return name if the entry contains a name, null otherwise
     */
    @Nullable
    String getName() {
      return mName;
    }

    @Override
    public boolean equals(Object other) {
      if (!checkReferenceAndClassEquality(this, other)) {
        return false;
      }
      NamePatternEntry entry = (NamePatternEntry) other;
      if (mIsPattern) {
        return Objects.equals(mPattern.pattern(), entry.mPattern.pattern());
      } else {
        return Objects.equals(mName, entry.mName);
      }
    }

    @Override
    public int hashCode() {
      if (mIsPattern) {
        return Objects.hashCode(mPattern.pattern());
      } else {
        return Objects.hashCode(mName);
      }
    }
  }

  /**
   * Deserializer of NamePatternEntry
   *
   * Accepts
   * 1. a plain name: "table1"
   * 2. an object of form: {"regex": "<regex>"}
   */
  static final class NamePatternEntryDeserializer extends JsonDeserializer<NamePatternEntry> {
    @Override
    public NamePatternEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      if (node == null) {
        return null;
      }
      if (node.isTextual()) {
        // a simple name
        return new NamePatternEntry(node.asText());
      }
      if (!node.isObject()) {
        throw new JsonParseException(
            mapper.treeAsTokens(node),
            String.format("invalid syntax, expecting name or an object with a `%s` field",
                NamePatternEntry.FIELD_REGEX)
        );
      }
      if (!node.hasNonNull(NamePatternEntry.FIELD_REGEX)) {
        throw new JsonParseException(
            mapper.treeAsTokens(node),
            String.format("invalid syntax, `%s` field missing or is null",
                NamePatternEntry.FIELD_REGEX)
        );
      }
      // a RegexObject object
      JsonNode regexNode = node.get(NamePatternEntry.FIELD_REGEX);
      try {
        Pattern regex = Pattern.compile(regexNode.asText());
        return new NamePatternEntry(regex);
      } catch (PatternSyntaxException e) {
        throw new JsonParseException(
            mapper.treeAsTokens(regexNode), "invalid regex syntax", e);
      }
    }
  }

  /**
   * A wrapper for included and excluded elements.
   * @param <INCLUDEDT> type of included entry
   */
  static class IncludeExcludeList<INCLUDEDT extends NamePatternEntry> {
    static final String FIELD_INCLUDE = "include";
    static final String FIELD_EXCLUDE = "exclude";

    @JsonProperty(FIELD_INCLUDE)
    private final Set<INCLUDEDT> mIncludedEntries;
    @JsonProperty(FIELD_EXCLUDE)
    private final Set<NamePatternEntry> mExcludedEntries;

    private static final IncludeExcludeList EMPTY_INSTANCE =
        new IncludeExcludeList(Collections.emptySet(), Collections.emptySet());

    /**
     * Creates an implicit include-only list.
     *
     * @param entries included {@link NamePatternEntry}s
     */
    IncludeExcludeList(@Nullable Set<INCLUDEDT> entries) {
      this(entries, Collections.emptySet());
    }

    /**
     * Creates an empty list.
     * @param <T> type of contained entry
     * @return an empty list
     */
    static <T extends NamePatternEntry> IncludeExcludeList<T> empty() {
      return (IncludeExcludeList<T>) EMPTY_INSTANCE;
    }

    boolean isEmpty() {
      return mIncludedEntries.isEmpty() && mExcludedEntries.isEmpty();
    }

    @JsonCreator
    IncludeExcludeList(
        @JsonProperty(FIELD_INCLUDE) @Nullable Set<INCLUDEDT> included,
        @JsonProperty(FIELD_EXCLUDE) @Nullable Set<NamePatternEntry> excluded) {
      mIncludedEntries = included == null ? Collections.emptySet() : included;
      mExcludedEntries = excluded == null ? Collections.emptySet() : excluded;
      // included and excluded cannot be both non-empty at the same time
      Preconditions.checkArgument(
          mIncludedEntries.isEmpty() || mExcludedEntries.isEmpty());
    }

    Set<NamePatternEntry> getExcludedEntries() {
      return mExcludedEntries;
    }

    Set<INCLUDEDT> getIncludedEntries() {
      return mIncludedEntries;
    }

    @Override
    public boolean equals(Object other) {
      if (!checkReferenceAndClassEquality(this, other)) {
        return false;
      }
      IncludeExcludeList list = (IncludeExcludeList) other;
      return Objects.equals(mIncludedEntries, list.mIncludedEntries)
          && Objects.equals(mExcludedEntries, list.mExcludedEntries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mIncludedEntries, mExcludedEntries);
    }
  }

  /**
   * Deserializer for IncludeExcludeList.
   * Handles the case with an implicit list of included entries.
   * @param <INCLUDEDT> the type for included entries
   */
  static class IncludeExcludeListDeserializer<INCLUDEDT extends NamePatternEntry> {
    IncludeExcludeList<INCLUDEDT> deserialize(
        Class<INCLUDEDT> type, JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      if (node == null) {
        return null;
      }
      if (node.isArray()) {
        // in case of an array, an included list is implied
        Set<INCLUDEDT> entries = mapper.convertValue(
            node,
            mapper.getTypeFactory().constructCollectionType(Set.class, type)
        );
        return new IncludeExcludeList<>(entries);
      }
      if (node.isObject()) {
        // otherwise, deserialize as an IncludeExcludeList object
        return mapper.convertValue(
            node,
            mapper.getTypeFactory().constructParametricType(IncludeExcludeList.class, type)
        );
      }
      throw new JsonParseException(mapper.treeAsTokens(node),
          "invalid syntax, expecting array or object");
    }
  }
}
