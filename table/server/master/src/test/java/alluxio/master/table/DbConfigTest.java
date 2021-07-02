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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.table.common.udb.UdbMountSpec;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import java.util.List;

public class DbConfigTest {
  private ObjectMapper mMapper;

  @Before
  public void before() {
    mMapper = new ObjectMapper();
  }

  /* TableEntry tests */
  @Test
  public void tableNamesOnly() throws Exception {
    DbConfig.TableEntry entry =
        mMapper.readValue("\"table1\"", DbConfig.TableEntry.class);
    assertEquals("table1", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void tableNamesAndPartitions() throws Exception {
    DbConfig.TableEntry entry = mMapper.readValue(
        "{\"table\": \"table2\", \"partitions\": [\"t2p1\", \"t2p2\"]}",
        DbConfig.TableEntry.class
    );
    assertEquals("table2", entry.getTable());
    assertEquals(ImmutableSet.of("t2p1", "t2p2"), entry.getPartitions());
  }

  @Test
  public void missingPartitions() throws Exception {
    DbConfig.TableEntry entry = mMapper.readValue(
        "{\"table\": \"table3\"}",
        DbConfig.TableEntry.class
    );
    assertEquals("table3", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void equalityRegardlessOfPartitions() throws Exception {
    DbConfig.TableEntry entry1 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p1\"]}",
        DbConfig.TableEntry.class
    );
    DbConfig.TableEntry entry2 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p2\"]}",
        DbConfig.TableEntry.class
    );
    assertEquals(entry1, entry2);
    assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  /* TablesEntry tests */
  @Test
  public void emptyListOfTables() throws Exception {
    DbConfig.TablesEntry entry = mMapper.readValue("{\"tables\": []}", DbConfig.TablesEntry.class);
    assertEquals(ImmutableSet.of(), entry.getTableNames());
  }

  @Test
  public void nullConstructor() throws Exception {
    DbConfig.TablesEntry entry1 = mMapper.readValue("{}", DbConfig.TablesEntry.class);
    assertEquals(ImmutableSet.of(), entry1.getTableNames());
    DbConfig.TablesEntry entry2 = new DbConfig.TablesEntry(null);
    assertEquals(ImmutableSet.of(), entry2.getTableNames());
  }

  /* DbConfig tests */
  @Test
  public void emptyConfig() throws Exception {
    List<String> src = ImmutableList.of(
        "{}",
        "{\"bypass\": {}}",
        "{\"ignore\": {}}",
        "{\"bypass\": {}, \"ignore\": {}}"
    );
    for (String input : src) {
      DbConfig config = mMapper.readValue(input, DbConfig.class);
      assertEquals(DbConfig.empty().getBypassEntry().getTableEntries(),
          config.getBypassEntry().getTableEntries());
      assertEquals(DbConfig.empty().getIgnoreEntry().getTableEntries(),
          config.getIgnoreEntry().getTableEntries());
    }
  }

  @Test
  public void convertToSpec() throws Exception {
    DbConfig config =
        mMapper.readValue("{\"bypass\": {\"tables\": [\"table1\"]}}", DbConfig.class);
    UdbMountSpec spec = config.getUdbMountSpec();
    assertTrue(spec.hasFullyBypassedTable("table1"));
    config =
        mMapper.readValue("{\"ignore\": {\"tables\": [\"table1\"]}}", DbConfig.class);
    spec = config.getUdbMountSpec();
    assertTrue(spec.hasIgnoredTable("table1"));
    assertEquals(ImmutableSet.of("table1"), spec.getIgnoredTables());
  }
}
