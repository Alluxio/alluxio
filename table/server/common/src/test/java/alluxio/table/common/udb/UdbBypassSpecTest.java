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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class UdbBypassSpecTest {
  @Test
  public void tableAndPartitionNames() {
    UdbBypassSpec spec = new UdbBypassSpec(
        ImmutableMap.of("table1", ImmutableSet.of("part1", "part2")));
    assertTrue(spec.hasTable("table1"));
    assertFalse(spec.hasFullTable("table1"));
    assertTrue(spec.hasPartition("table1", "part1"));
    assertTrue(spec.hasPartition("table1", "part2"));
    assertFalse(spec.hasPartition("table1", "part3"));
  }

  @Test
  public void tableNamesOnly() {
    UdbBypassSpec spec = new UdbBypassSpec(
        ImmutableMap.of("table2", ImmutableSet.of()));
    assertTrue(spec.hasTable("table2"));
    assertTrue(spec.hasFullTable("table2"));
    assertTrue(spec.hasPartition("table2", "part1"));
    assertTrue(spec.hasPartition("table2", "part2"));
    assertTrue(spec.hasPartition("table2", "part3"));
  }

  @Test
  public void nonExistentTable() {
    UdbBypassSpec spec = new UdbBypassSpec(
        ImmutableMap.of("table3", ImmutableSet.of()));
    assertFalse(spec.hasTable("table4"));
    assertFalse(spec.hasFullTable("table4"));
    assertFalse(spec.hasPartition("table4", "part1"));
    assertFalse(spec.hasPartition("table4", "part2"));
    assertFalse(spec.hasPartition("table4", "part3"));
  }
}
