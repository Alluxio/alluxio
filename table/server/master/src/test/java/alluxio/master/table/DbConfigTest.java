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

import static alluxio.master.table.DbConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.table.common.udb.UdbMountSpec;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

public class DbConfigTest {
  private ObjectMapper mMapper;

  @Before
  public void before() {
    mMapper = new ObjectMapper();
  }

  /* TableEntry tests */
  @Test
  public void tableNamesOnly() throws Exception {
    TableEntry entry =
        mMapper.readValue("\"table1\"", TableEntry.class);
    assertEquals("table1", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void tableNamesAndPartitions() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"table\": \"table2\", \"partitions\": [\"t2p1\", \"t2p2\"]}",
        TableEntry.class
    );
    assertEquals("table2", entry.getTable());
    assertEquals(ImmutableSet.of("t2p1", "t2p2"), entry.getPartitions());
  }

  @Test
  public void missingPartitions() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"table\": \"table3\"}",
        TableEntry.class
    );
    assertEquals("table3", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void equalityRegardlessOfPartitions() throws Exception {
    TableEntry entry1 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p1\"]}",
        TableEntry.class
    );
    TableEntry entry2 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p2\"]}",
        TableEntry.class
    );
    assertEquals(entry1, entry2);
    assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  /* TablesEntry tests */
  /* IgnoreTablesEntry is a type alias for TablesEntry<NameEntry> */
  @Test
  public void emptyListOfTables() throws Exception {
    TablesEntry entry = mMapper.readValue("{\"tables\": []}", TablesEntry.class);
    assertEquals(ImmutableSet.of(), entry.getTableNames());
  }

  @Test
  public void nullConstructor() throws Exception {
    IgnoreTablesEntry entry2 = new IgnoreTablesEntry(null);
    assertEquals(ImmutableSet.of(), entry2.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry2.getList().getExcludedEntries());
  }
  
  @Test
  public void implicitIncludeList() throws Exception {
    IgnoreTablesEntry entry =
        mMapper.readValue(
            "{\"tables\": [\"table1\"]}", 
            new TypeReference<IgnoreTablesEntry>() {});
    assertEquals(ImmutableSet.of(new NameEntry("table1")), entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  @Test
  public void explicitIncludeExcludeList() throws Exception {
    IgnoreTablesEntry entry =
        mMapper.readValue(
            "{\"tables\": {\"include\": [\"table1\"]}}",
            new TypeReference<IgnoreTablesEntry>() {});
    assertEquals(ImmutableSet.of(new NameEntry("table1")), entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  @Test
  public void bypassTablesEntry() throws Exception {
    BypassTablesEntry entry =
        mMapper.readValue(
            "{\"tables\": [{\"table\": \"table1\"}]}",
            new TypeReference<BypassTablesEntry>() {});
    assertEquals(ImmutableSet.of(new TableEntry("table1")),
        entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  /* DbConfig tests */
  @Test
  public void convertToSpec() throws Exception {
    // Todo(bowen): add more tests to cover partitions, inclusion/exclusion
    DbConfig config =
        mMapper.readValue("{\"bypass\": {\"tables\": [\"table1\"]}}", DbConfig.class);
    UdbMountSpec spec = config.getUdbMountSpec();
    assertTrue(spec.hasFullyBypassedTable("table1"));
    config =
        mMapper.readValue("{\"ignore\": {\"tables\": [\"table1\"]}}", DbConfig.class);
    spec = config.getUdbMountSpec();
    assertTrue(spec.hasIgnoredTable("table1"));
  }
}
