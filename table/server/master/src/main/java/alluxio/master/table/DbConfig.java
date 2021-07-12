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
 * Top level: DbConfig
 * DbConfig := {"bypass": BypassTablesSpec, "ignore": IgnoreTablesSpec}
 * BypassTablesSpec := {"tables": BypassIncludeTablesList | BypassTablesObject}
 * IgnoreTablesSpec := {"tables": SimpleNameRegexList | SimpleIncludeExcludeObject}
 * BypassTablesObject := {"include": BypassIncludeTablesList} | {"exclude": SimpleNameRegexList}
 * BypassIncludeTablesList := [ NameLiteral | RegexObject | BypassTablePartitionSpec ]*
 * SimpleNameRegexList := [ NameLiteral | RegexObject ]*
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
  private final BypassTablesSpec mBypassEntry;
  private final IgnoreTablesSpec mIgnoreEntry;

  /**
   * @param bypassEntry bypass entry
   * @param ignoreEntry ignore entry
   */
  @JsonCreator
  public DbConfig(@JsonProperty("bypass") @Nullable BypassTablesSpec bypassEntry,
                  @JsonProperty("ignore") @Nullable IgnoreTablesSpec ignoreEntry) {
    mBypassEntry = bypassEntry == null ? new BypassTablesSpec(null) : bypassEntry;
    mIgnoreEntry = ignoreEntry == null ? new IgnoreTablesSpec(null) : ignoreEntry;
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
  public TablesEntry<NamePatternEntry> getIgnoreEntry() {
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
      IncludeExcludeList<NamePatternEntry> partitions = entry.getPartitions();
      UdbMountSpec.SimpleWrapperBuilder partitionBuilder = new UdbMountSpec.SimpleWrapperBuilder();
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
      builder.bypass().include().addPartition(entry.getTable(), partitionBuilder);
    }

    for (NamePatternEntry entry : mBypassEntry.getList().getExcludedEntries()) {
      if (entry.isPattern()) {
        builder.bypass().exclude().addPattern(entry.getPattern());
      } else {
        builder.bypass().exclude().addName(entry.getName());
      }
    }

    for (NamePatternEntry entry : mIgnoreEntry.getList().getIncludedEntries()) {
      if (entry.isPattern()) {
        builder.ignore().include().addPattern(entry.getPattern());
      } else {
        builder.ignore().include().addName(entry.getName());
      }
    }

    for (NamePatternEntry entry : mIgnoreEntry.getList().getExcludedEntries()) {
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
  @JsonDeserialize(using = BypassTablesSpecDeserializer.class)
  public static final class BypassTablesSpec extends TablesEntry<TableEntry> {
    // inherited: IncludeExcludeList<TableEntry>
    // this is the BypassTablesObject from syntax specification

    /**
     * @param list list of table entries
     */
    public BypassTablesSpec(@Nullable IncludeExcludeList<TableEntry> list) {
      super(list);
    }
  }

  /**
   * Type alias for TablesEntry<NamePatternEntry>.
   */
  @JsonDeserialize(using = IgnoreTablesSpecDeserializer.class)
  public static final class IgnoreTablesSpec extends TablesEntry<NamePatternEntry> {
    // inherited: IncludeExcludeList<NamePatternEntry>
    // this is the SimpleIncludeExcludeObject from syntax specification

    /**
     * @param list list of table entries
     */
    public IgnoreTablesSpec(@Nullable IncludeExcludeList<NamePatternEntry> list) {
      super(list);
    }
  }

  /**
   * The "tables" object: {"tables": ... }.
   * Base class for BypassTablesSpec and IgnoreTablesSpec.
   * @param <T> the type of entry contained
   */
  public static  class TablesEntry<T extends NamePatternEntry> {
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
      if (!node.hasNonNull("tables")) {
        throw new JsonParseException(mapper.treeAsTokens(node), "missing field `tables`");
      }
      node = node.get("tables");
      if (node.isArray()) {
        // in case of an array, an included list is implied
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

  static class IgnoreTablesSpecDeserializer extends JsonDeserializer<IgnoreTablesSpec> {
    @Override
    public IgnoreTablesSpec deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      TablesEntryDeserializer<NamePatternEntry> deserializer = new TablesEntryDeserializer<>();
      IncludeExcludeList<NamePatternEntry> list =
          deserializer.deserializeToList(NamePatternEntry.class, jp, cxt);
      return new IgnoreTablesSpec(list);
    }
  }

  static class BypassTablesSpecDeserializer extends JsonDeserializer<BypassTablesSpec> {
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
  public static class TableEntry extends NamePatternEntry {
    private final IncludeExcludeList<NamePatternEntry> mPartitions;

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
    public TableEntry(String tableName, IncludeExcludeList<NamePatternEntry> partitions) {
      super(tableName);
      mPartitions = partitions;
    }

    /**
     * Creates an instance from a {@link NamePatternEntry} with no partition specification.
     * @param namePatternEntry name entry
     */
    public TableEntry(NamePatternEntry namePatternEntry) {
      super(namePatternEntry);
      mPartitions = IncludeExcludeList.empty();
    }

    /**
     * Returns partition specifications.
     * @return partitions
     */
    public IncludeExcludeList<NamePatternEntry> getPartitions() {
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
      return super.equals(other);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  // Accepts a simple table name, an regular expression, or
  // an object of form: {"table": "tableName", "partitions": ["part1", "part2"]}
  static class TableEntryDeserializer extends JsonDeserializer<TableEntry> {
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
      if (node.hasNonNull("table")) {
        String tableName = node.get("table").asText();
        JsonNode partitionsList = node.get("partitions");
        if (partitionsList == null) {
          return new TableEntry(tableName);
        }
        IncludeExcludeList<NamePatternEntry> partitions;
        if (partitionsList.isArray()) {
          // an implicit included list
          Set<NamePatternEntry> includedPartitions =
              mapper.convertValue(partitionsList, new TypeReference<Set<NamePatternEntry>>() {});
          partitions = new IncludeExcludeList<>(includedPartitions);
        } else {
          // an IncludeExcludeList object
          partitions = mapper.convertValue(
              partitionsList, new TypeReference<IncludeExcludeList<NamePatternEntry>>() {});
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
   * Wrapper of a explicit name or a regular expression.
   */
  @JsonDeserialize(using = NamePatternEntryDeserializer.class)
  public static class NamePatternEntry {
    private final boolean mIsPattern;
    private final Pattern mPattern;
    private final String mName;

    /**
     * Creates an entry with a simple name.
     *
     * @param name table or partition name
     */
    public NamePatternEntry(String name) {
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
    public NamePatternEntry(Pattern regex) {
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
  static class NamePatternEntryDeserializer extends JsonDeserializer<NamePatternEntry> {
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
      if (!node.isObject() || !node.hasNonNull("regex")) {
        throw new JsonParseException(mapper.treeAsTokens(node),
            "invalid syntax, expecting name or an object with a `regex` key");
      }
      // a RegexObject object
      try {
        Pattern regex = Pattern.compile(node.get("regex").asText());
        return new NamePatternEntry(regex);
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
  public static class IncludeExcludeList<INCLUDEDT extends NamePatternEntry> {
    @JsonProperty("include")
    private final Set<INCLUDEDT> mIncludedEntries;
    @JsonProperty("exclude")
    private final Set<NamePatternEntry> mExcludedEntries;

    /**
     * Creates an implicit include-only list.
     *
     * @param entries included {@link NamePatternEntry}s
     */
    public IncludeExcludeList(@Nullable Set<INCLUDEDT> entries) {
      this(entries, Collections.emptySet());
    }

    /**
     * Creates an empty list.
     * @param <T> type of contained entry
     * @return an empty list
     */
    public static <T extends NamePatternEntry> IncludeExcludeList<T> empty() {
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
     * @param included included {@link NamePatternEntry}s
     * @param excluded excluded {@link NamePatternEntry}s
     */
    @JsonCreator
    public IncludeExcludeList(
        @JsonProperty("include") @Nullable Set<INCLUDEDT> included,
        @JsonProperty("exclude") @Nullable Set<NamePatternEntry> excluded) {
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
    public Set<NamePatternEntry> getExcludedEntries() {
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
