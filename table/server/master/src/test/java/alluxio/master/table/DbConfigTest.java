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

import alluxio.table.common.udb.UdbBypassSpec;

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

  /* BypassTableEntry tests */
  @Test
  public void tableNamesOnly() throws Exception {
    DbConfig.BypassTableEntry entry =
        mMapper.readValue("\"table1\"", DbConfig.BypassTableEntry.class);
    assertEquals("table1", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void tableNamesAndPartitions() throws Exception {
    DbConfig.BypassTableEntry entry = mMapper.readValue(
        "{\"table\": \"table2\", \"partitions\": [\"t2p1\", \"t2p2\"]}",
        DbConfig.BypassTableEntry.class
    );
    assertEquals("table2", entry.getTable());
    assertEquals(ImmutableSet.of("t2p1", "t2p2"), entry.getPartitions());
  }

  @Test
  public void missingPartitions() throws Exception {
    DbConfig.BypassTableEntry entry = mMapper.readValue(
        "{\"table\": \"table3\"}",
        DbConfig.BypassTableEntry.class
    );
    assertEquals("table3", entry.getTable());
    assertEquals(ImmutableSet.of(), entry.getPartitions());
  }

  @Test
  public void equalityRegardlessOfPartitions() throws Exception {
    DbConfig.BypassTableEntry entry1 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p1\"]}",
        DbConfig.BypassTableEntry.class
    );
    DbConfig.BypassTableEntry entry2 = mMapper.readValue(
        "{\"table\": \"table4\", \"partitions\": [\"p2\"]}",
        DbConfig.BypassTableEntry.class
    );
    assertEquals(entry1, entry2);
    assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  /* BypassEntry tests */
  @Test
  public void emptyListOfTables() throws Exception {
    DbConfig.BypassEntry entry = mMapper.readValue("{\"tables\": []}", DbConfig.BypassEntry.class);
    assertEquals(ImmutableSet.of(), entry.getBypassedTables());
  }

  @Test
  public void nullConstructor() throws Exception {
    DbConfig.BypassEntry entry1 = mMapper.readValue("{}", DbConfig.BypassEntry.class);
    assertEquals(ImmutableSet.of(), entry1.getBypassedTables());
    DbConfig.BypassEntry entry2 = new DbConfig.BypassEntry(null);
    assertEquals(ImmutableSet.of(), entry2.getBypassedTables());
  }

  @Test
  public void convertToUdbBypassSpec() throws Exception {
    DbConfig.BypassEntry entry =
        mMapper.readValue("{\"tables\": [\"table1\"]}", DbConfig.BypassEntry.class);
    assertEquals(ImmutableSet.of("table1"), entry.getBypassedTables());
    UdbBypassSpec spec = entry.toUdbBypassSpec();
    assertTrue(spec.hasTable("table1"));
  }

  /* DbConfig tests */
  @Test
  public void emptyConfig() throws Exception {
    List<String> src = ImmutableList.of(
        "{}",
        "{\"bypass\": {}}"
    );
    for (String input : src) {
      DbConfig config = mMapper.readValue(input, DbConfig.class);
      assertEquals(DbConfig.empty().getBypassEntry().getBypassTableEntries(),
          config.getBypassEntry().getBypassTableEntries());
    }
  }
}
