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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * The Alluxio db config information.
 */
public final class DbConfig {
  private final TablesEntry mBypassEntry;
  private final TablesEntry mIgnoreEntry;

  /**
   * @param bypassEntry bypass entry
   * @param ignoreEntry ignore entry
   */
  @JsonCreator
  public DbConfig(@JsonProperty("bypass") @Nullable TablesEntry bypassEntry,
                  @JsonProperty("ignore") @Nullable TablesEntry ignoreEntry) {
    mBypassEntry = bypassEntry == null ? new TablesEntry(Collections.emptySet()) : bypassEntry;
    mIgnoreEntry = ignoreEntry == null ? new TablesEntry(Collections.emptySet()) : ignoreEntry;
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
  public TablesEntry getBypassEntry() {
    return mBypassEntry;
  }

  /**
   * @return the {@link TablesEntry} for ignored tables from config file
   */
  public TablesEntry getIgnoreEntry() {
    return mIgnoreEntry;
  }

  /**
   * @return the {@link UdbMountSpec} object
   */
  public UdbMountSpec getUdbMountSpec() {
    Map<String, Set<String>> bypassed = mBypassEntry.getTableEntries().stream().collect(
        Collectors.toMap(TableEntry::getTable, TableEntry::getPartitions));
    Set<String> ignored = mIgnoreEntry.getTableNames();
    return new UdbMountSpec(bypassed, ignored);
  }

  /**
   * Tables configuration entry from config file.
   */
  public static final class TablesEntry {
    @JsonProperty("tables")
    private final Set<TableEntry> mEntries;

    /**
     * @param entries set of {@link TableEntry}s
     */
    @JsonCreator
    public TablesEntry(@JsonProperty("tables") @Nullable Set<TableEntry> entries) {
      mEntries = entries == null ? Collections.emptySet() : entries;
    }

    /**
     * @return table names
     */
    public Set<String> getTableNames() {
      return mEntries.stream().map(TableEntry::getTable).collect(Collectors.toSet());
    }

    /**
     * @return {@link TableEntry}s
     */
    public Set<TableEntry> getTableEntries() {
      return mEntries;
    }
  }

  /**
   * Table to partitions mapping.
   */
  @JsonDeserialize(using = TableEntryDeserializer.class)
  public static class TableEntry {
    private final String mTableName;
    private final Set<String> mPartitions;

    /**
     * @param tableName table name
     * @param partitions partition names
     */
    @JsonCreator
    public TableEntry(@JsonProperty("table") String tableName,
                      @JsonProperty("partitions") Set<String> partitions) {
      Preconditions.checkArgument(!tableName.isEmpty(), "empty table name");
      mTableName = tableName;
      mPartitions = partitions;
    }

    /**
     * @return table name
     */
    public String getTable() {
      return mTableName;
    }

    /**
     * @return partition names
     */
    public Set<String> getPartitions() {
      return mPartitions;
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
      TableEntry entry = (TableEntry) other;
      return Objects.equals(mTableName, entry.mTableName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mTableName);
    }
  }

  /**
   * Deserializer of TableEntry
   *
   * Enables flexible syntax: either a single table name can be specified, and all belonging
   * partitions will be bypassed;
   * or an object of form
   * {"table": "tableName", "partitions": ["part1", "part2"]}
   * can be used, and individual partitions can be specified.
   */
  public static class TableEntryDeserializer extends JsonDeserializer<TableEntry> {
    @Override
    public TableEntry deserialize(JsonParser jp, DeserializationContext cxt)
        throws IOException, JsonProcessingException {
      ObjectMapper mapper = (ObjectMapper) jp.getCodec();
      JsonNode node = mapper.readTree(jp);
      String tableName;
      Set<String> partitions;
      if (!node.isTextual() && !node.isObject()) {
        throw new JsonParseException(mapper.treeAsTokens(node), "invalid syntax");
      } else if (node.isTextual()) {
        // single table name, all partitions are bypassed
        tableName = node.asText();
        partitions = Collections.emptySet();
      } else {
        // a {"table": "table", "partitions": ["part1", "part2"]} object
        if (!node.hasNonNull("table")) {
          throw new JsonParseException(mapper.treeAsTokens(node), "missing table name");
        }
        tableName = node.get("table").asText();
        JsonNode partitionsList = node.get("partitions");
        partitions = mapper.convertValue(partitionsList,  new TypeReference<Set<String>>() {});
        if (partitions == null) {
          partitions = Collections.emptySet();
        }
      }
      return new TableEntry(tableName, partitions);
    }
  }
}
