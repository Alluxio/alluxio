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

import alluxio.table.common.udb.UdbFilterSpec.Mode;

import org.junit.Before;
import org.junit.Test;

import java.util.regex.Pattern;

public class UdbFilterSpecTest {
  private UdbFilterSpec.Builder mBuilder;

  @Before
  public void before() {
    mBuilder = new UdbFilterSpec.Builder();
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
    mBuilder.setBypassedPartitionsMode(getTableName(1), Mode.INCLUDE)
        .addBypassedPartition(getTableName(1), getPartitionName(1))
        .addBypassedPartition(getTableName(1), getPartitionName(2));

    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(2)));
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(3)));
  }

  @Test
  public void includedTableNamesOnly() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(getTableName(2));
    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(2)));
    assertTrue(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(2)));
    assertTrue(spec.isBypassedPartition(getTableName(2), getPartitionName(3)));
  }

  @Test
  public void includedNonExistentTable() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(getTableName(3));
    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(getTableName(4)));
    assertFalse(spec.isFullyBypassedTable(getTableName(4)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(1)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(2)));
    assertFalse(spec.isBypassedPartition(getTableName(4), getPartitionName(3)));
  }

  @Test
  public void includedTablePatterns() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(Pattern.compile(getTableName("\\d")));
    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(getTableName(1)));
    assertTrue(spec.isBypassedTable(getTableName(2)));
    assertTrue(spec.isFullyBypassedTable(getTableName(1)));
    assertTrue(spec.isFullyBypassedTable(getTableName(2)));
  }

  @Test
  public void includedTableMixedNamesPatternsPartitions() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(getTableName(1))
        .addBypassedTable(Pattern.compile(getTableName("[a-z]")))
        .setBypassedPartitionsMode(getTableName(2), Mode.INCLUDE)
        .addBypassedPartition(getTableName(2), getPartitionName(1))
        .addBypassedPartition(getTableName(2), getPartitionName(2))
        .addBypassedPartition(getTableName(2), Pattern.compile(getPartitionName("[a-z]")));

    UdbFilterSpec spec = mBuilder.build();
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
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(getTableName(1))
        .setBypassedPartitionsMode(getTableName(1), Mode.INCLUDE)
        .addBypassedPartition(getTableName(1), getPartitionName(1));
    assertThrows(IllegalStateException.class, () -> mBuilder.build());
  }

  @Test
  public void rejectExcludedTableWithPartitionSpec() {
    mBuilder.setBypassedTablesMode(Mode.EXCLUDE)
        .addBypassedTable(getTableName(1))
        .setBypassedPartitionsMode(getTableName(2), Mode.INCLUDE)
        .addBypassedPartition(getTableName(2), getPartitionName(1));
    assertThrows(IllegalStateException.class, () -> mBuilder.build());
  }

  /* Exclusion tests */
  @Test
  public void excludedTableNamesOnly() {
    mBuilder.setBypassedTablesMode(Mode.EXCLUDE)
        .addBypassedTable(getTableName(1))
        .addBypassedTable(getTableName(2));

    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isBypassedTable(getTableName(2)));
    assertFalse(spec.isFullyBypassedTable(getTableName(1)));
    assertFalse(spec.isFullyBypassedTable(getTableName(2)));
    assertTrue(spec.isBypassedTable(getTableName(3)));
    assertTrue(spec.isFullyBypassedTable(getTableName(3)));
  }

  @Test
  public void excludedTableNamesPatterns() {
    mBuilder.setBypassedTablesMode(Mode.EXCLUDE)
        .addBypassedTable(getTableName(0))
        .addBypassedTable(Pattern.compile(getTableName("[12]")));

    UdbFilterSpec spec = mBuilder.build();
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
    mBuilder.setBypassedPartitionsMode(getTableName(1), Mode.EXCLUDE)
        .addBypassedPartition(getTableName(1), getPartitionName(1));

    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
    assertTrue(spec.isBypassedPartition(getTableName(1), getPartitionName(2)));
  }

  @Test
  public void excludeEverythingIsIncludeNothing() {
    mBuilder.setBypassedTablesMode(Mode.EXCLUDE)
        .addBypassedTable(Pattern.compile(".*"));
    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  @Test
  public void excludeNothingIsIncludeNothing() {
    mBuilder.setBypassedTablesMode(Mode.EXCLUDE);
    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  /* Ignoring tests */
  @Test
  public void ignoredTables() {
    mBuilder.setIgnoredTablesMode(Mode.INCLUDE)
        .addIgnoredTable(getTableName(1))
        .addIgnoredTable(Pattern.compile(getTableName(2)));

    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(getTableName(1)));
    assertTrue(spec.isIgnoredTable(getTableName(2)));
    assertFalse(spec.isIgnoredTable(getTableName(3)));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable("same_table")
        .setIgnoredTablesMode(Mode.INCLUDE)
        .addIgnoredTable("same_table");
    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("same_table"));
    assertFalse(spec.isBypassedTable("same_table"));
    assertFalse(spec.isBypassedPartition("same_table", ANY_PART));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass2() {
    mBuilder.setBypassedTablesMode(Mode.INCLUDE)
        .addBypassedTable(getTableName(1))
        .setIgnoredTablesMode(Mode.EXCLUDE)
        .addIgnoredTable(getTableName(2));
    UdbFilterSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(getTableName(1)));
    assertFalse(spec.isBypassedTable(getTableName(1)));
    assertFalse(spec.isBypassedPartition(getTableName(1), ANY_PART));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypassPartitions() {
    mBuilder.setBypassedPartitionsMode(getTableName(1), Mode.INCLUDE)
        .addBypassedPartition(getTableName(1), getPartitionName(1))
        .setIgnoredTablesMode(Mode.INCLUDE)
        .addIgnoredTable(getTableName(1));
    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition(getTableName(1), getPartitionName(1)));
  }

  @Test
  public void ignoreNone() {
    mBuilder.setIgnoredTablesMode(Mode.INCLUDE);
    // no tables added
    UdbFilterSpec spec = mBuilder.build();
    assertFalse(spec.isIgnoredTable(ANY_TABLE));
  }
}
