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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.regex.Pattern;

public class UdbAttachOptionsTest {
  private UdbAttachOptions.Builder mBuilder;

  @Before
  public void before() {
    mBuilder = new UdbAttachOptions.Builder();
  }

  private static final String ANY_TABLE = "any_table";
  private static final String ANY_PART = "any_part";

  private static String getTableName(int i) {
    return String.format("table%d", i);
  }

  private static String getTableName(String suffix) {
    return String.format("table%s", suffix);
  }

  private static String getPartitionName(int i) {
    return String.format("part%d", i);
  }

  private static String getPartitionName(String suffix) {
    return String.format("part%s", suffix);
  }

  /* Bypassing tests */
  /* Inclusion tests */
  @Test
  public void includedTableAndPartitionNames() {
    mBuilder.bypass()
        .include()
        .addPartition(getTableName(1), getPartitionName(1))
        .addPartition(getTableName(1), getPartitionName(2));

    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(2)));
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(3)));
  }

  @Test
  public void includedTableNamesOnly() {
    mBuilder.bypass().include().addTable(getTableName(2));
    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(2)));
    assertTrue(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(2)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(3)));
  }

  @Test
  public void includedNonExistentTable() {
    mBuilder.bypass().include().addTable(getTableName(3));
    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(getTableName(4)));
    assertFalse(spec.isFullyBypassedTable(getTableName(4)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(1)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(2)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(3)));
  }

  @Test
  public void includedTablePatterns() {
    mBuilder.bypass().include().addTable(Pattern.compile(getTableName("\\d")));
    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(1)));
    assertTrue(spec.isBypassedTable(getTableName(2)));
    assertTrue(spec.isFullyBypassedTable(getTableName(1)));
    assertTrue(spec.isFullyBypassedTable(getTableName(2)));
  }

  @Test
  public void includedTableMixedNamesPatternsPartitions() {
    mBuilder.bypass()
        .include()
        .addTable(getTableName(1))
        .addTable(Pattern.compile(getTableName("[a-z]")))
        .addPartition(getTableName(2), getPartitionName(1))
        .addPartition(getTableName(2), getPartitionName(2))
        .addPartition(getTableName(2), Pattern.compile(getPartitionName("[a-z]")));

    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(1)));
    assertTrue(spec.isBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedTable(getTableName("a")));
    assertTrue(spec.isFullyBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isFullyBypassedTable(getTableName("a")));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(2)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName("a")));
    assertTrue(spec.isBypassedPartition(getTableName("a"), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName("a"), getPartitionName(2)));
    assertTrue(spec.isBypassedPartition(getTableName("a"), getPartitionName("a")));
  }

  @Test
  public void rejectTableExplicitNameAndPartitionSpecAtSameTime() {
    mBuilder.bypass().include().addTable(getTableName(1));
    assertThrows(IllegalStateException.class,
        () -> mBuilder.addPartition(getTableName(1), getPartitionName(1)));
  }

  @Test
  public void rejectInclusionExclusionAtSameTime() {
    mBuilder.bypass().include().addTable(getTableName(1));
    assertThrows(IllegalStateException.class,
        () -> mBuilder.bypass().exclude().addTable(getTableName(2)));
  }

  @Test
  public void rejectInclusionExclusionAtSameTime2() {
    mBuilder.bypass().include().addPartition(getTableName(1), getPartitionName(1));
    assertThrows(IllegalStateException.class,
        () -> mBuilder.bypass().exclude().addPartition(getTableName(1), getPartitionName(2)));
  }

  /* Exclusion tests */
  @Test
  public void excludedTableNamesOnly() {
    mBuilder.bypass().exclude().addTables(ImmutableSet.of(getTableName(1), getTableName(2)));

    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isBypassedTable(getTableName(2)));
    assertFalse(spec.isFullyBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedTable(getTableName(3)));
    assertTrue(spec.isFullyBypassedTable(getTableName(3)));
  }

  @Test
  public void excludedTableNamesPatterns() {
    mBuilder.bypass()
        .exclude()
        .addTable(getTableName(0))
        .addTable(Pattern.compile(getTableName("[12]")));

    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(getTableName(0)));
    assertFalse(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isBypassedTable(getTableName(2)));
    assertFalse(spec.isFullyBypassedTable(getTableName(0)));
    assertFalse(spec.isFullyBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedTable(getTableName(3)));
    assertTrue(spec.isFullyBypassedTable(getTableName(3)));
  }

  @Test
  public void excludedPartitionsOfIncludedTable() {
    mBuilder.bypass()
        .exclude()
        .addPartition(getTableName(1), getPartitionName(1));

    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(2)));
  }

  @Test
  public void excludeEverythingIsIncludeNothing() {
    mBuilder.bypass().exclude().addTable(Pattern.compile(".*"));
    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  @Test
  public void excludeNothingIsIncludeNothing() {
    mBuilder.bypass().exclude().addTables(Collections.emptySet());
    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  /* Ignoring tests */
  @Test
  public void ignoredTables() {
    mBuilder.ignore()
        .include()
        .addTable(getTableName(1))
        .addTables(ImmutableSet.of(getTableName(2), getTableName(3)))
        .addTable(Pattern.compile(getTableName(4)))
        .addTables(ImmutableSet.of(
            Pattern.compile(getTableName(5)),
            Pattern.compile(getTableName(6))
        ));

    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(getTableName(1)));
    assertTrue(spec.isIgnoredTable(getTableName(2)));
    assertTrue(spec.isIgnoredTable(getTableName(3)));
    assertTrue(spec.isIgnoredTable(getTableName(4)));
    assertTrue(spec.isIgnoredTable(getTableName(5)));
    assertTrue(spec.isIgnoredTable(getTableName(6)));
    assertFalse(spec.isIgnoredTable(getTableName(7)));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass() {
    mBuilder.bypass().include().addTable("same_table");
    mBuilder.ignore().include().addTable("same_table");
    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("same_table"));
    assertFalse(spec.isBypassedTable("same_table"));
    assertFalse(spec.isBypassedPartition("same_table", ANY_PART));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass2() {
    mBuilder.bypass().include().addTable(getTableName(1));
    mBuilder.ignore().exclude().addTable(getTableName(2));
    UdbAttachOptions spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(getTableName(1)));
    assertFalse(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isBypassedPartition(getTableName(1), ANY_PART));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypassPartitions() {
    mBuilder.bypass().include().addPartition(getTableName(1), getPartitionName(1));
    mBuilder.ignore().include().addTable(getTableName(1));
    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
  }

  @Test
  public void ignoreNone() {
    mBuilder.ignore().include().addTables(ImmutableSet.of());
    UdbAttachOptions spec = mBuilder.build();
    assertFalse(spec.isIgnoredTable(ANY_TABLE));
  }
}
