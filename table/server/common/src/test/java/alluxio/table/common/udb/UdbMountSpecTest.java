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

package alluxio.table.common.udb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class UdbMountSpecTest {
  /* Bypassing tests */
  @Test
  public void tableAndPartitionNames() {
    UdbMountSpec spec = new UdbMountSpec(
        ImmutableMap.of("table1", ImmutableSet.of("part1", "part2")),
        ImmutableSet.of());
    assertTrue(spec.hasBypassedTable("table1"));
    assertFalse(spec.hasFullyBypassedTable("table1"));
    assertTrue(spec.hasBypassedPartition("table1", "part1"));
    assertTrue(spec.hasBypassedPartition("table1", "part2"));
    assertFalse(spec.hasBypassedPartition("table1", "part3"));
  }

  @Test
  public void tableNamesOnly() {
    UdbMountSpec spec = new UdbMountSpec(
        ImmutableMap.of("table2", ImmutableSet.of()),
        ImmutableSet.of());
    assertTrue(spec.hasBypassedTable("table2"));
    assertTrue(spec.hasFullyBypassedTable("table2"));
    assertTrue(spec.hasBypassedPartition("table2", "part1"));
    assertTrue(spec.hasBypassedPartition("table2", "part2"));
    assertTrue(spec.hasBypassedPartition("table2", "part3"));
  }

  @Test
  public void nonExistentTable() {
    UdbMountSpec spec = new UdbMountSpec(
        ImmutableMap.of("table3", ImmutableSet.of()),
        ImmutableSet.of());
    assertFalse(spec.hasBypassedTable("table4"));
    assertFalse(spec.hasFullyBypassedTable("table4"));
    assertFalse(spec.hasBypassedPartition("table4", "part1"));
    assertFalse(spec.hasBypassedPartition("table4", "part2"));
    assertFalse(spec.hasBypassedPartition("table4", "part3"));
  }

  /* Ignoring tests */
  @Test
  public void ignoredTables() {
    UdbMountSpec spec = new UdbMountSpec(
        ImmutableMap.of(),
        ImmutableSet.of("table1"));
    assertTrue(spec.hasIgnoredTable("table1"));
    assertEquals(ImmutableSet.of("table1"), spec.getIgnoredTables());
  }
}
