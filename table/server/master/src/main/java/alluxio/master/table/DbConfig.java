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

import alluxio.table.common.udb.UdbMountSpec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;

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
 * Current syntax:
 * Top level: BypassIgnoreObject
 * BypassIgnoreObject := {"bypass": BypassTablesSpec, "ignore": IgnoreTablesSpec}
 * BypassTablesSpec := {"tables": BypassIncludeTablesList | BypassTablesObject}
 * IgnoreTablesSpec := {"tables": SimpleNameRegexList | SimpleTablesObject}
 * BypassTablesObject := {"include": BypassIncludeTablesList} | {"exclude": SimpleNameRegexList}
 * BypassIncludeTablesList := [ NameLiteral | RegexObject | BypassTablePartitionSpec ]*
 * SimpleNameRegexList := [ NameLiteral | RegexObject ]*
 * SimpleTablesObject := SimpleIncludeExcludeObject
 * SimpleIncludeExcludeObject := {"include": SimpleNameRegexList} | {"exclude": SimpleNameRegexList}
 * BypassTablePartitionSpec :=
 *     {"table": NameLiteral, "partition": SimpleNameRegexList | SimpleIncludeExcludeObject}
 * RegexObject := {"regex": RegexLiteral}
 *
 * An example:
 * {                  <- BypassIgnoreObject
 *   "bypass": {        <- BypassTablesSpec
 *     "tables": {        <- BypassTablesObject
 *       "include": [       <- BypassIncludeTablesList
 *         "table1",          <- NameLiteral
 *         {                    <- BypassTablePartitionSpec
 *           "table": "table2",
 *           "partition": {         <- SimpleIncludeExcludeObject
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
 *   "ignore": {        <- IgnoreTablesSpec
 *     "tables": ["table4"]
 *   }
 * }
 */
public final class DbConfig {
  private final BypassTablesEntry mBypassEntry;
  private final IgnoreTablesEntry mIgnoreEntry;

  /**
   * @param bypassEntry bypass entry
   * @param ignoreEntry ignore entry
   */
  @JsonCreator
  public DbConfig(@JsonProperty("bypass") @Nullable BypassTablesEntry bypassEntry,
                  @JsonProperty("ignore") @Nullable IgnoreTablesEntry ignoreEntry) {
    mBypassEntry = bypassEntry == null ? new BypassTablesEntry(null) : bypassEntry;
    mIgnoreEntry = ignoreEntry == null ? new IgnoreTablesEntry(null) : ignoreEntry;
  }

  /**
   * Returns an empty configuration.
   *
   * @return an empty config instance
   */
  public static DbConfig empty() {
    return new DbConfig(null, null);
  }

  /**
   * @return the {@link TablesEntry} for bypassed tables from config file
   */
  public TablesEntry<TableEntry> getBypassEntry() {
    return mBypassEntry;
  }

  /**
   * @return the {@link TablesEntry} for ignored tables from config file
   */
  public TablesEntry<NameEntry> getIgnoreEntry() {
    return mIgnoreEntry;
  }

  /**
   * @return the {@link UdbMountSpec} object
   */
  public UdbMountSpec getUdbMountSpec() {
    UdbMountSpec.Builder builder = new UdbMountSpec.Builder();
    for (TableEntry entry : mBypassEntry.getList().getIncludedEntries()) {
      if (entry.isPattern()) {
        builder.bypass().include().addPattern(entry.getPattern());
        continue;
      }
      if (entry.getPartitions().isEmpty()) {
        builder.bypass().include().addName(entry.getName());
        continue;
      }
      IncludeExcludeList<NameEntry> partitions = entry.getPartitions();
      UdbMountSpec.SimpleWrapperBuilder partitionBuilder = new UdbMountSpec.SimpleWrapperBuilder();
      for (NameEntry partition : partitions.getIncludedEntries()) {
        if (partition.isPattern()) {
          partitionBuilder.include().addPattern(partition.getPattern());
        } else {
          partitionBuilder.include().addName(partition.getName());
        }
      }
      for (NameEntry partition : partitions.getExcludedEntries()) {
        if (partition.isPattern()) {
          partitionBuilder.exclude().addPattern(partition.getPattern());
        } else {
          partitionBuilder.exclude().addName(partition.getName());
        }
      }
      builder.bypass().include().addPartition(entry.getTable(), partitionBuilder);
    }

    for (NameEntry entry : mBypassEntry.getList().getExcludedEntries()) {
      if (entry.isPattern()) {
        builder.bypass().exclude().addPattern(entry.getPattern());
      } else {
        builder.bypass().exclude().addName(entry.getName());
      }
    }

    for (NameEntry entry : mIgnoreEntry.getList().getIncludedEntries()) {
      if (entry.isPattern()) {
        builder.ignore().include().addPattern(entry.getPattern());
      } else {
        builder.ignore().include().addName(entry.getName());
      }
    }

    for (NameEntry entry : mIgnoreEntry.getList().getExcludedEntries()) {
      if (entry.isPattern()) {
        builder.ignore().exclude().addPattern(entry.getPattern());
      } else {
        builder.ignore().exclude().addName(entry.getName());
      }
    }
    return builder.build();
  }

  /**
   * Type alias for TablesEntry<TableEntry>.
   */
  @JsonDeserialize(using = BypassTablesEntryDeserializer.class)
  public static final class BypassTablesEntry extends TablesEntry<TableEntry> {

    /**
     * @param list list of table entries
     */
    public BypassTablesEntry(@Nullable IncludeExcludeList<TableEntry> list) {
      super(list);
    }
  }

  /**
   * Type alias for TablesEntry<NameEntry>.
   */
  @JsonDeserialize(using = IgnoreTablesEntryDeserializer.class)
  public static final class IgnoreTablesEntry extends TablesEntry<NameEntry> {

    /**
     * @param list list of table entries
     */
    public IgnoreTablesEntry(@Nullable IncludeExcludeList<NameEntry> list) {
      super(list);
    }
  }

  /**
   * Tables configuration entry from config file.
   * @param <T> the type of entry contained
   */
  public static  class TablesEntry<T extends NameEntry> {
    private final IncludeExcludeList<T> mList;

    /**
     * @param list list of table entries
     */
    public TablesEntry(@Nullable IncludeExcludeList<T> list) {
      mList = list == null ? new IncludeExcludeList<>(Collections.emptySet()) : list;
    }

    /**
     * Returns the list of tables.
     * @return list of tables
     */
    public IncludeExcludeList<T> getList() {
      return mList;
    }
  }

  /**
   * Tables entry accepts one the two following:
   * 1. a list of {@link NameEntry}s
   * 2. an object containing `include`, `exclude` and `includeFirstOnConflict` keys
   */
  static class TablesEntryDeserializer<T extends NameEntry> {
    IncludeExcludeList<T> deserializeToList(
        Class<T> type, JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      if (node == null) {
        return null;
      }
      if (!node.hasNonNull("tables")) {
        throw new JsonParseException(mapper.treeAsTokens(node), "missing field `tables`");
      }
      node = node.get("tables");
      if (node.isArray()) {
        // in case an array, an included list is implied
        Set<T> entries = mapper.convertValue(
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

  static class IgnoreTablesEntryDeserializer extends JsonDeserializer<IgnoreTablesEntry> {
    @Override
    public IgnoreTablesEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      TablesEntryDeserializer<NameEntry> deserializer = new TablesEntryDeserializer<>();
      IncludeExcludeList<NameEntry> list =
          deserializer.deserializeToList(NameEntry.class, jp, cxt);
      return new IgnoreTablesEntry(list);
    }
  }

  static class BypassTablesEntryDeserializer extends JsonDeserializer<BypassTablesEntry> {
    @Override
    public BypassTablesEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      TablesEntryDeserializer<TableEntry> deserializer = new TablesEntryDeserializer<>();
      IncludeExcludeList<TableEntry> list =
          deserializer.deserializeToList(TableEntry.class, jp, cxt);
      return new BypassTablesEntry(list);
    }
  }

  /**
   * Contains additional partition specification.
   * If the set of partitions is empty, all belonging partitions of that table will be bypassed.
   */
  @JsonDeserialize(using = TableEntryDeserializer.class)
  public static class TableEntry extends NameEntry {
    private final IncludeExcludeList<NameEntry> mPartitions;

    /**
     * Creates an instance with a specific table name and no partition specification.
     *
     * @param tableName table name
     */
    public TableEntry(String tableName) {
      this(tableName, IncludeExcludeList.empty());
    }

    /**
     * Creates an instance with a specific table name and possibly partitions specifications.
     *
     * @param tableName table name
     * @param partitions partitions
     */
    public TableEntry(String tableName, IncludeExcludeList<NameEntry> partitions) {
      super(tableName);
      mPartitions = partitions;
    }

    /**
     * Creates an instance from a {@link NameEntry} with no partition specification.
     * @param nameEntry name entry
     */
    public TableEntry(NameEntry nameEntry) {
      super(nameEntry);
      mPartitions = IncludeExcludeList.empty();
    }

    /**
     * Returns partition specifications.
     * @return partitions
     */
    public IncludeExcludeList<NameEntry> getPartitions() {
      return mPartitions;
    }

    /**
     * Returns table name if the entry is not a regex entry.
     * @return table name, null if the entry is a regex entry
     */
    @Nullable
    public String getTable() {
      return getName();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (getClass() != other.getClass()) {
        return false;
      }
      if (!super.equals(other)) {
        return false;
      }
      TableEntry entry = (TableEntry) other;
      return Objects.equals(mPartitions, entry.mPartitions);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + Objects.hashCode(mPartitions);
    }
  }

  /**
   * Accepts a simple table name or
   * an object of form: {"table": "tableName", "partitions": ["part1", "part2"]}
   * the specified individual partitions will be bypasses. Any others, if any, will not.
   */
  static class TableEntryDeserializer extends JsonDeserializer<TableEntry> {
    @Override
    public TableEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      // try deserialize as a `NameEntry` object first
      try {
        NameEntryDeserializer deserializer = new NameEntryDeserializer();
        NameEntry nameEntry =  deserializer.deserialize(mapper.treeAsTokens(node), cxt);
        return new TableEntry(nameEntry);
      } catch (JsonProcessingException e) {
        // ignore, and try deserialize as a `TableEntry` object
      }
      if (node == null) {
        return null;
      }
      // a {"table": "table", "partitions": ["part1", "part2"]} object
      if (node.hasNonNull("table")) {
        String tableName = node.get("table").asText();
        JsonNode partitionsList = node.get("partitions");
        if (partitionsList == null) {
          return new TableEntry(tableName);
        }
        IncludeExcludeList<NameEntry> partitions;
        if (partitionsList.isArray()) {
          // an implicit included list
          Set<NameEntry> includedPartitions =
              mapper.convertValue(partitionsList, new TypeReference<Set<NameEntry>>() {});
          partitions = new IncludeExcludeList<>(includedPartitions);
        } else {
          // an IncludeExcludeList object
          partitions = mapper.convertValue(
              partitionsList, new TypeReference<IncludeExcludeList<NameEntry>>() {});
          if (partitions == null) {
            partitions = IncludeExcludeList.empty();
          }
        }
        return new TableEntry(tableName, partitions);
      }
      throw new JsonParseException(mapper.treeAsTokens(node),
          "invalid syntax, expecting table name, regex, or an object with a `table` key");
    }
  }

  /**
   * Name entry for table names and partition names.
   * Comes in two flavors: a simple name and regular expressions.
   */
  @JsonDeserialize(using = NameEntryDeserializer.class)
  public static class NameEntry {
    private final boolean mIsPattern;
    private final Pattern mPattern;
    private final String mName;

    /**
     * Creates an entry with a simple name.
     *
     * @param name table or partition name
     */
    public NameEntry(String name) {
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
    public NameEntry(Pattern regex) {
      mIsPattern = true;
      mPattern = regex;
      mName = null;
    }

    /**
     * Copy constructor.
     *
     * @param nameEntry instance to copy
     */
    NameEntry(NameEntry nameEntry) {
      mIsPattern = nameEntry.mIsPattern;
      mName = nameEntry.mName;
      mPattern = nameEntry.mPattern;
    }

    /**
     * @return if the entry is a regex entry
     */
    public boolean isPattern() {
      return mIsPattern;
    }

    /**
     * @return pattern if the entry contains a regex pattern
     */
    @Nullable
    public Pattern getPattern() {
      return mPattern;
    }

    /**
     * @return table name
     */
    @Nullable
    public String getName() {
      return mName;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (getClass() != other.getClass()) {
        return false;
      }
      NameEntry entry = (NameEntry) other;
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
   * Deserializer of NameEntry
   *
   * Accepts
   * 1. a plain name: "table1"
   * 2. an object of form: {"regex": "<regex>"}
   */
  static class NameEntryDeserializer extends JsonDeserializer<NameEntry> {
    @Override
    public NameEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      if (node == null) {
        return null;
      }
      if (node.isTextual()) {
        // a simple name
        return new NameEntry(node.asText());
      }
      if (!node.isObject() || !node.hasNonNull("regex")) {
        throw new JsonParseException(mapper.treeAsTokens(node),
            "invalid syntax, expecting name or an object with a `regex` key");
      }
      // a {"regex": "<regex>"} object
      try {
        Pattern regex = Pattern.compile(node.get("regex").asText());
        return new NameEntry(regex);
      } catch (PatternSyntaxException e) {
        throw new JsonParseException(
            mapper.treeAsTokens(node.get("regex")), "invalid regex syntax", e);
      }
    }
  }

  /**
   * A wrapper for included and excluded elements.
   * @param <INCLUDEDT> type of included entry
   */
  public static class IncludeExcludeList<INCLUDEDT extends NameEntry> {
    @JsonProperty("include")
    private final Set<INCLUDEDT> mIncludedEntries;
    @JsonProperty("exclude")
    private final Set<NameEntry> mExcludedEntries;

    /**
     * Creates an implicit include-only list.
     *
     * @param entries included {@link NameEntry}s
     */
    public IncludeExcludeList(@Nullable Set<INCLUDEDT> entries) {
      this(entries, Collections.emptySet());
    }

    /**
     * Creates an empty list.
     * @param <T> type of contained entry
     * @return an empty list
     */
    public static <T extends NameEntry> IncludeExcludeList<T> empty() {
      return new IncludeExcludeList<>(Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Checks if the list is empty.
     * @return if the list is empty
     */
    public boolean isEmpty() {
      return mIncludedEntries.isEmpty() && mExcludedEntries.isEmpty();
    }

    /**
     * Json creator.
     *
     * @param included included {@link NameEntry}s
     * @param excluded excluded {@link NameEntry}s
     */
    @JsonCreator
    public IncludeExcludeList(
        @JsonProperty("include") @Nullable Set<INCLUDEDT> included,
        @JsonProperty("exclude") @Nullable Set<NameEntry> excluded) {
      mIncludedEntries = included == null ? Collections.emptySet() : included;
      mExcludedEntries = excluded == null ? Collections.emptySet() : excluded;
      // included and excluded cannot be both non-empty at the same time
      Preconditions.checkArgument(
          mIncludedEntries.isEmpty() || mExcludedEntries.isEmpty());
    }

    /**
     * Returns excluded entries.
     * @return excluded entries
     */
    public Set<NameEntry> getExcludedEntries() {
      return mExcludedEntries;
    }

    /**
     * Returns included entries.
     * @return included entries
     */
    public Set<INCLUDEDT> getIncludedEntries() {
      return mIncludedEntries;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null) {
        return false;
      } else if (getClass() != other.getClass()) {
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
}
