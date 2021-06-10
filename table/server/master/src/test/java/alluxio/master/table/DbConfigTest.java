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
import static org.junit.Assert.assertFalse;
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

  @Test
  public void empty() throws Exception {
    List<String> src = ImmutableList.of(
        "{}",
        "{\"bypass\": null}",
        "{\"bypass\": {}}",
        "{\"bypass\": {\"tables\": []}}"
    );
    for (String input : src) {
      DbConfig config = mMapper.readValue(input, DbConfig.class);
      assertEquals(0, config.getBypassEntry().getBypassedTables().size());
    }
  }

  @Test
  public void tableNamesOnly() throws Exception {
    DbConfig config = mMapper.readValue(
        "{\"bypass\": {\"tables\": [\"table1\", \"table2\"]}}", DbConfig.class);
    assertEquals(ImmutableSet.of("table1", "table2"), config.getBypassEntry().getBypassedTables());
  }

  @Test
  public void tableNamesAndPartitions() throws Exception {
    DbConfig config = mMapper.readValue(
        "{\"bypass\": {\"tables\": [\"table1\", {\"table\": \"table2\", "
        + "\"partitions\": [\"t2p1\", \"t2p2\"]}]}}",
        DbConfig.class
    );
    UdbBypassSpec spec = config.getUdbBypassSpec();
    assertTrue(spec.isBypassedTable("table1"));
    assertTrue(spec.isFullyBypassedTable("table1"));

    assertTrue(spec.isBypassedTable("table2"));
    assertFalse(spec.isFullyBypassedTable("table2"));

    assertTrue(spec.isBypassedPartition("table1", "t1p1"));
    assertTrue(spec.isBypassedPartition("table1", "t1p2"));

    assertTrue(spec.isBypassedPartition("table2", "t2p1"));
    assertTrue(spec.isBypassedPartition("table2", "t2p2"));
    assertFalse(spec.isBypassedPartition("table2", "t2p3"));
  }

  @Test
  public void missingPartitions() throws Exception {
    DbConfig config = mMapper.readValue(
        "{\"bypass\": {\"tables\": [{\"table\": \"table1\"}]}}",
        DbConfig.class
    );
    assertTrue(config.getUdbBypassSpec().isFullyBypassedTable("table1"));
  }
}
