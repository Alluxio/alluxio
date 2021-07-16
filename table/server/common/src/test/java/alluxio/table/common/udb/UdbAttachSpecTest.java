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

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.regex.Pattern;

public class UdbAttachSpecTest {
  private UdbAttachSpec.Builder mBuilder;

  @Before
  public void before() {
    mBuilder = new UdbAttachSpec.Builder();
  }

  /* Bypassing tests */
  /* Inclusion tests */
  @Test
  public void includedTableAndPartitionNames() {
    UdbAttachSpec.PartitionSpecBuilder partitionBuilder = new UdbAttachSpec.PartitionSpecBuilder();
    partitionBuilder.include().addName("part1").addName("part2");
    mBuilder.bypass()
        .include()
        .addPartition("table1", partitionBuilder);

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable("table1"));
    assertFalse(spec.isFullyBypassedTable("table1"));
    assertTrue(spec.isBypassedPartition("table1", "part1"));
    assertTrue(spec.isBypassedPartition("table1", "part2"));
    assertFalse(spec.isBypassedPartition("table1", "part3"));
  }

  @Test
  public void includedTableNamesOnly() {
    mBuilder.bypass().include().addName("table2");
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable("table2"));
    assertTrue(spec.isFullyBypassedTable("table2"));
    assertTrue(spec.isBypassedPartition("table2", "part1"));
    assertTrue(spec.isBypassedPartition("table2", "part2"));
    assertTrue(spec.isBypassedPartition("table2", "part3"));
  }

  @Test
  public void includedNonExistentTable() {
    mBuilder.bypass().include().addName("table3");
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable("table4"));
    assertFalse(spec.isFullyBypassedTable("table4"));
    assertFalse(spec.isBypassedPartition("table4", "part1"));
    assertFalse(spec.isBypassedPartition("table4", "part2"));
    assertFalse(spec.isBypassedPartition("table4", "part3"));
  }

  @Test
  public void includedTablePatterns() {
    mBuilder.bypass().include().addPattern(Pattern.compile("table\\d"));
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable("table1"));
    assertTrue(spec.isBypassedTable("table2"));
    assertTrue(spec.isFullyBypassedTable("table1"));
    assertTrue(spec.isFullyBypassedTable("table2"));
  }

  @Test
  public void includedTableMixedNamesPatternsPartitions() {
    UdbAttachSpec.PartitionSpecBuilder partitionBuilder = new UdbAttachSpec.PartitionSpecBuilder();
    partitionBuilder
        .include()
        .addNames(ImmutableSet.of("part1", "part2"))
        .addPattern(Pattern.compile("part_[a-z]"));
    mBuilder.bypass()
        .include()
        .addName("table1")
        .addPattern(Pattern.compile("table_[a-z]]"))
        .addPartition("table2", partitionBuilder);

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable("table1"));
    assertTrue(spec.isBypassedTable("table2"));
    assertTrue(spec.isFullyBypassedTable("table1"));
    assertFalse(spec.isFullyBypassedTable("table2"));
    assertTrue(spec.isBypassedPartition("table2", "part1"));
    assertTrue(spec.isBypassedPartition("table2", "part2"));
    assertTrue(spec.isBypassedPartition("table2", "part_a"));
  }

  @Test
  public void includedTableExplicitNamesFirst() {
    UdbAttachSpec.PartitionSpecBuilder partitionBuilder = new UdbAttachSpec.PartitionSpecBuilder();
    partitionBuilder.include().addName("part1");
    mBuilder.bypass()
        .include()
        .addName("table1")
        .addPartition("table1", partitionBuilder);

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isFullyBypassedTable("table1"));
    assertTrue(spec.isBypassedPartition("table1", "part2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectInclusionExclusionAtSameTime() {
    mBuilder.bypass().include().addName("table1");
    mBuilder.bypass().exclude().addName("table2");
    mBuilder.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectInclusionExclusionAtSameTime2() {
    UdbAttachSpec.PartitionSpecBuilder partitionBuilder = new UdbAttachSpec.PartitionSpecBuilder();
    partitionBuilder.include().addName("part1");
    partitionBuilder.exclude().addName("part2");
    partitionBuilder.build();
  }

  /* Exclusion tests */
  @Test
  public void excludedTableNamesOnly() {
    mBuilder.bypass().exclude().addNames(ImmutableSet.of("table1", "table2"));

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable("table1"));
    assertFalse(spec.isBypassedTable("table2"));
    assertFalse(spec.isFullyBypassedTable("table1"));
    assertFalse(spec.isFullyBypassedTable("table2"));
    assertTrue(spec.isBypassedTable("table3"));
    assertTrue(spec.isFullyBypassedTable("table3"));
  }

  @Test
  public void excludedTableNamesPatterns() {
    mBuilder.bypass()
        .exclude()
        .addName("table0")
        .addPattern(Pattern.compile("table[12]"));

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable("table0"));
    assertFalse(spec.isBypassedTable("table1"));
    assertFalse(spec.isBypassedTable("table2"));
    assertFalse(spec.isFullyBypassedTable("table0"));
    assertFalse(spec.isFullyBypassedTable("table1"));
    assertFalse(spec.isFullyBypassedTable("table2"));
    assertTrue(spec.isBypassedTable("table3"));
    assertTrue(spec.isFullyBypassedTable("table3"));
  }

  @Test
  public void excludedPartitionsOfIncludedTable() {
    UdbAttachSpec.PartitionSpecBuilder partitionBuilder = new UdbAttachSpec.PartitionSpecBuilder();
    partitionBuilder.exclude().addName("part1");
    mBuilder.bypass()
        .include()
        .addPartition("table1", partitionBuilder);

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition("table1", "part1"));
    assertTrue(spec.isBypassedPartition("table1", "part2"));
  }

  @Test
  public void excludeEverythingIsIncludeNothing() {
    mBuilder.bypass().exclude().addPattern(Pattern.compile(".*"));
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable("any_table"));
  }

  @Test
  public void excludeNothingIsIncludeNothing() {
    mBuilder.bypass()
        .exclude().addNames(Collections.emptySet()).addPatterns(Collections.emptySet());
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable("any_table"));
  }

  /* Ignoring tests */
  @Test
  public void ignoredTables() {
    mBuilder.ignore()
        .include()
        .addName("table1")
        .addNames(ImmutableSet.of("table2", "table3"))
        .addPattern(Pattern.compile("table4"))
        .addPatterns(ImmutableSet.of(
            Pattern.compile("table5"),
            Pattern.compile("table6")
        ));

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("table1"));
    assertTrue(spec.isIgnoredTable("table2"));
    assertTrue(spec.isIgnoredTable("table3"));
    assertTrue(spec.isIgnoredTable("table4"));
    assertTrue(spec.isIgnoredTable("table5"));
    assertTrue(spec.isIgnoredTable("table6"));
    assertFalse(spec.isIgnoredTable("table7"));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass() {
    mBuilder.bypass().include().addName("same_table");
    mBuilder.ignore().include().addName("same_table");
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("same_table"));
    assertFalse(spec.isBypassedTable("same_table"));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass2() {
    mBuilder.bypass().include().addName("table1");
    mBuilder.ignore().exclude().addName("table2");
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("table1"));
    assertFalse(spec.isBypassedTable("table1"));
  }
}
