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

import static alluxio.master.table.DbConfig.BypassTablesSpec;
import static alluxio.master.table.DbConfig.IgnoreTablesSpec;
import static alluxio.master.table.DbConfig.IncludeExcludeList;
import static alluxio.master.table.DbConfig.NamePatternEntry;
import static alluxio.master.table.DbConfig.TableEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.table.common.udb.UdbAttachSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

public class DbConfigTest {
  private ObjectMapper mMapper;

  @Before
  public void before() {
    mMapper = new ObjectMapper();
  }

  /* NameEntry tests */
  @Test
  public void simpleName() throws Exception {
    NamePatternEntry entry = mMapper.readValue("\"table1\"", NamePatternEntry.class);
    assertFalse(entry.isPattern());
    assertEquals("table1", entry.getName());
  }

  @Test
  public void regexPattern() throws Exception {
    NamePatternEntry entry = mMapper.readValue(
        "{\"regex\":\"^table\\\\d$\"}", NamePatternEntry.class);
    assertTrue(entry.isPattern());
    assertEquals("^table\\d$", entry.getPattern().pattern());
  }

  @Test(expected = JsonProcessingException.class)
  public void rejectUnknownKey() throws Exception {
    mMapper.readValue("{\"some_key\":\"some_value\"}", NamePatternEntry.class);
  }

  @Test(expected = JsonProcessingException.class)
  public void rejectBadPattern() throws Exception {
    mMapper.readValue("{\"regex\":\"unclosed parenthesis (\"}", NamePatternEntry.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectEmptyName() throws Exception {
    mMapper.readValue("\"\"", NamePatternEntry.class);
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
            new NamePatternEntry("t2p1"),
            new NamePatternEntry("t2p2")
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
        ImmutableSet.of(new NamePatternEntry("t2p1")),
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
        new TypeReference<IncludeExcludeList<NamePatternEntry>>() {});
  }

  @Test
  public void includeOnlyNameEntry() throws Exception {
    IncludeExcludeList<NamePatternEntry> list =
        mMapper.readValue(
            "{\"include\": [\"table1\"]}",
            new TypeReference<IncludeExcludeList<NamePatternEntry>>() {});
    assertEquals(ImmutableSet.of(new NamePatternEntry("table1")), list.getIncludedEntries());
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
    IncludeExcludeList<NamePatternEntry> list =
        mMapper.readValue(
            "{\"exclude\": [\"table1\"]}",
            new TypeReference<IncludeExcludeList<NamePatternEntry>>() {});
    assertEquals(ImmutableSet.of(new NamePatternEntry("table1")), list.getExcludedEntries());
    assertEquals(ImmutableSet.of(), list.getIncludedEntries());
  }

  /* TablesEntry tests */
  /* IgnoreTablesEntry is a type alias for TablesEntry<NameEntry> */
  @Test
  public void emptyListOfTables() throws Exception {
    IgnoreTablesSpec entry =
        mMapper.readValue(
            "{\"tables\": []}",
            new TypeReference<IgnoreTablesSpec>() {});
    assertEquals(ImmutableSet.of(), entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  @Test
  public void nullConstructor() throws Exception {
    IgnoreTablesSpec entry2 = new IgnoreTablesSpec(null);
    assertEquals(ImmutableSet.of(), entry2.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry2.getList().getExcludedEntries());
  }

  @Test
  public void implicitIncludeList() throws Exception {
    IgnoreTablesSpec entry =
        mMapper.readValue(
            "{\"tables\": [\"table1\"]}",
            new TypeReference<IgnoreTablesSpec>() {});
    assertEquals(
        ImmutableSet.of(new NamePatternEntry("table1")),
        entry.getList().getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  @Test
  public void explicitIncludeExcludeList() throws Exception {
    IgnoreTablesSpec entry =
        mMapper.readValue(
            "{\"tables\": {\"include\": [\"table1\"]}}",
            new TypeReference<IgnoreTablesSpec>() {});
    assertEquals(
        ImmutableSet.of(new NamePatternEntry("table1")),
        entry.getList().getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  @Test
  public void bypassTablesEntry() throws Exception {
    BypassTablesSpec entry =
        mMapper.readValue(
            "{\"tables\": [{\"table\": \"table1\"}]}",
            new TypeReference<BypassTablesSpec>() {});
    assertEquals(ImmutableSet.of(new TableEntry("table1")),
        entry.getList().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getList().getExcludedEntries());
  }

  /* DbConfig tests */
  @Test
  public void convertToSpec() throws Exception {
    InputStream stream = getClass().getResourceAsStream("/DbConfigTestSample.json");
    DbConfig config =
        mMapper.readValue(stream, DbConfig.class);
    UdbAttachSpec spec = config.getUdbAttachSpec();
    assertTrue(spec.hasFullyBypassedTable("bypassed1"));
    assertTrue(spec.hasFullyBypassedTable("bypassed_regex1"));
    assertTrue(spec.hasFullyBypassedTable("bypassed_regex9"));
    assertTrue(spec.hasBypassedTable("bypassed2"));
    assertFalse(spec.hasFullyBypassedTable("bypassed2"));
    assertFalse(spec.hasBypassedTable("any_other_table"));
    assertFalse(spec.hasFullyBypassedTable("any_other_table"));
    assertFalse(spec.hasBypassedPartition("bypassed2", "part1"));
    assertFalse(spec.hasBypassedPartition("bypassed2", "part_regex1"));
    assertFalse(spec.hasBypassedPartition("bypassed2", "part_regex9"));
    assertTrue(spec.hasBypassedPartition("bypassed2", "any_other_part"));
    assertTrue(spec.hasIgnoredTable("ignored3"));
    assertTrue(spec.hasIgnoredTable("ignored_regex1"));
    assertTrue(spec.hasIgnoredTable("ignored_regex9"));
    assertFalse(spec.hasIgnoredTable("any_other_table"));
    assertTrue(spec.hasIgnoredTable("bypassed_and_ignored"));
    assertFalse(spec.hasBypassedTable("bypassed_and_ignored"));
  }
}
