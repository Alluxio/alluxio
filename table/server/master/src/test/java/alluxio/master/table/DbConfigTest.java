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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.table.common.udb.UdbMountSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

public class DbConfigTest {
  private ObjectMapper mMapper;

  @Before
  public void before() {
    mMapper = new ObjectMapper();
  }

  /* NameEntry tests */
  @Test
  public void simpleName() throws Exception {
    NameEntry entry = mMapper.readValue("\"table1\"", NameEntry.class);
    assertFalse(entry.isPattern());
    assertEquals("table1", entry.getName());
  }

  @Test
  public void regexPattern() throws Exception {
    NameEntry entry = mMapper.readValue(
        "{\"regex\":\"^table\\\\d$\"}", NameEntry.class);
    assertTrue(entry.isPattern());
    assertEquals("^table\\d$", entry.getPattern().pattern());
  }

  @Test(expected = JsonProcessingException.class)
  public void rejectUnknownKey() throws Exception {
    mMapper.readValue("{\"some_key\":\"some_value\"}", NameEntry.class);
  }

  @Test(expected = JsonProcessingException.class)
  public void rejectBadPattern() throws Exception {
    mMapper.readValue("{\"regex\":\"unclosed parenthesis (\"}", NameEntry.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectEmptyName() throws Exception {
    mMapper.readValue("\"\"", NameEntry.class);
  }

  /* TableEntry tests */
  @Test
  public void tableNamesOnly() throws Exception {
    TableEntry entry =
        mMapper.readValue("\"table1\"", TableEntry.class);
    assertFalse(entry.isPattern());
    assertEquals("table1", entry.getTable());
    assertEquals(IncludeExcludeList.empty(), entry.getPartitions());
  }

  @Test
  public void tableNamesAndPartitions() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"table\": \"table2\", \"partitions\": [\"t2p1\", \"t2p2\"]}",
        TableEntry.class
    );
    assertFalse(entry.isPattern());
    assertEquals("table2", entry.getTable());
    assertEquals(
        ImmutableSet.of(
            new NameEntry("t2p1"),
            new NameEntry("t2p2")
        ),
        entry.getPartitions().getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getPartitions().getExcludedEntries());
  }

  @Test
  public void partitionsAsIncludeExcludeList() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"table\": \"table2\", \"partitions\": {\"exclude\": [\"t2p1\"]}}",
        TableEntry.class
    );
    assertFalse(entry.isPattern());
    assertEquals("table2", entry.getTable());
    assertEquals(
        ImmutableSet.of(new NameEntry("t2p1")),
        entry.getPartitions().getExcludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getPartitions().getIncludedEntries());
  }

  @Test
  public void missingPartitions() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"table\": \"table3\"}",
        TableEntry.class
    );
    assertFalse(entry.isPattern());
    assertEquals("table3", entry.getTable());
    assertEquals(IncludeExcludeList.empty(), entry.getPartitions());
  }

  @Test
  public void regexPatternAsTable() throws Exception {
    TableEntry entry = mMapper.readValue(
        "{\"regex\": \"^table\\\\d\"}",
        TableEntry.class
    );
    assertTrue(entry.isPattern());
    assertEquals(IncludeExcludeList.empty(), entry.getPartitions());
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

  /* IncludeExcludeList tests */
  @Test(expected = JsonProcessingException.class)
  public void rejectIncludeAndExcludeNameEntry() throws Exception {
    mMapper.readValue(
        "{\"include\": [\"table1\"], \"exclude\": [\"table2\"]}",
        new TypeReference<IncludeExcludeList<NameEntry>>() {});
  }

  @Test
  public void includeOnlyNameEntry() throws Exception {
    IncludeExcludeList<NameEntry> list =
        mMapper.readValue(
            "{\"include\": [\"table1\"]}",
            new TypeReference<IncludeExcludeList<NameEntry>>() {});
    assertEquals(ImmutableSet.of(new NameEntry("table1")), list.getIncludedEntries());
    assertEquals(ImmutableSet.of(), list.getExcludedEntries());
  }

  @Test
  public void includeOnlyTableEntry() throws Exception {
    IncludeExcludeList<TableEntry> list =
        mMapper.readValue(
            "{\"include\": [\"table1\", {\"table\": \"table2\"}]}",
            new TypeReference<IncludeExcludeList<TableEntry>>() {});
    assertEquals(
        ImmutableSet.of(
            new TableEntry("table1"),
            new TableEntry("table2")
        ),
        list.getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), list.getExcludedEntries());
  }

  @Test
  public void excludeOnlyNameEntry() throws Exception {
    IncludeExcludeList<NameEntry> list =
        mMapper.readValue(
            "{\"exclude\": [\"table1\"]}",
            new TypeReference<IncludeExcludeList<NameEntry>>() {});
    assertEquals(ImmutableSet.of(new NameEntry("table1")), list.getExcludedEntries());
    assertEquals(ImmutableSet.of(), list.getIncludedEntries());
  }

  /* TablesEntry tests */
  /* IgnoreTablesEntry is a type alias for TablesEntry<NameEntry> */
  @Test
  public void emptyListOfTables() throws Exception {
    IgnoreTablesEntry entry =
        mMapper.readValue(
            "{\"tables\": []}", 
            new TypeReference<IgnoreTablesEntry>() {});
    assertEquals(ImmutableSet.of(), entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
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
