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

  private static final String ANY_TABLE = "any_table";
  private static final String ANY_PART = "any_part";

  private static String TABLE(int i) {
    return String.format("table%d", i);
  }

  private static String TABLE(String suffix) {
    return String.format("table%s", suffix);
  }

  private static String PART(int i) {
    return String.format("part%d", i);
  }

  private static String PART(String suffix) {
    return String.format("part%s", suffix);
  }

  /* Bypassing tests */
  /* Inclusion tests */
  @Test
  public void includedTableAndPartitionNames() {
    mBuilder.bypass()
        .include()
        .addPartition(TABLE(1), PART(1))
        .addPartition(TABLE(1), PART(2));

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(TABLE(1)));
    assertFalse(spec.isFullyBypassedTable(TABLE(1)));
    assertTrue(spec.isBypassedPartition(TABLE(1), PART(1)));
    assertTrue(spec.isBypassedPartition(TABLE(1), PART(2)));
    assertFalse(spec.isBypassedPartition(TABLE(1), PART(3)));
  }

  @Test
  public void includedTableNamesOnly() {
    mBuilder.bypass().include().addTable(TABLE(2));
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(TABLE(2)));
    assertTrue(spec.isFullyBypassedTable(TABLE(2)));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART(1)));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART(2)));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART(3)));
  }

  @Test
  public void includedNonExistentTable() {
    mBuilder.bypass().include().addTable(TABLE(3));
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(TABLE(4)));
    assertFalse(spec.isFullyBypassedTable(TABLE(4)));
    assertFalse(spec.isBypassedPartition(TABLE(4), PART(1)));
    assertFalse(spec.isBypassedPartition(TABLE(4), PART(2)));
    assertFalse(spec.isBypassedPartition(TABLE(4), PART(3)));
  }

  @Test
  public void includedTablePatterns() {
    mBuilder.bypass().include().addTable(Pattern.compile(TABLE("\\d")));
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(TABLE(1)));
    assertTrue(spec.isBypassedTable(TABLE(2)));
    assertTrue(spec.isFullyBypassedTable(TABLE(1)));
    assertTrue(spec.isFullyBypassedTable(TABLE(2)));
  }

  @Test
  public void includedTableMixedNamesPatternsPartitions() {
    mBuilder.bypass()
        .include()
        .addTable(TABLE(1))
        .addTable(Pattern.compile(TABLE("[a-z]")))
        .addPartition(TABLE(2), PART(1))
        .addPartition(TABLE(2), PART(2))
        .addPartition(TABLE(2), Pattern.compile(PART("[a-z]")));

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isBypassedTable(TABLE(1)));
    assertTrue(spec.isBypassedTable(TABLE(2)));
    assertTrue(spec.isBypassedTable(TABLE("a")));
    assertTrue(spec.isFullyBypassedTable(TABLE(1)));
    assertFalse(spec.isFullyBypassedTable(TABLE(2)));
    assertTrue(spec.isFullyBypassedTable(TABLE("a")));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART(1)));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART(2)));
    assertTrue(spec.isBypassedPartition(TABLE(2), PART("a")));
    assertTrue(spec.isBypassedPartition(TABLE("a"), PART(1)));
    assertTrue(spec.isBypassedPartition(TABLE("a"), PART(2)));
    assertTrue(spec.isBypassedPartition(TABLE("a"), PART("a")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectTableExplicitNameAndPartitionSpecAtSameTime() {
    mBuilder.bypass()
        .include()
        .addTable(TABLE(1))
        .addPartition(TABLE(1), PART(1));

    mBuilder.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectInclusionExclusionAtSameTime() {
    mBuilder.bypass().include().addTable(TABLE(1));
    mBuilder.bypass().exclude().addTable(TABLE(2));
    mBuilder.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectInclusionExclusionAtSameTime2() {
    mBuilder.bypass().include().addPartition(TABLE(1), PART(1));
    mBuilder.bypass().exclude().addPartition(TABLE(1), PART(2));
    mBuilder.build();
  }

  /* Exclusion tests */
  @Test
  public void excludedTableNamesOnly() {
    mBuilder.bypass().exclude().addTables(ImmutableSet.of(TABLE(1), TABLE(2)));

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(TABLE(1)));
    assertFalse(spec.isBypassedTable(TABLE(2)));
    assertFalse(spec.isFullyBypassedTable(TABLE(1)));
    assertFalse(spec.isFullyBypassedTable(TABLE(2)));
    assertTrue(spec.isBypassedTable(TABLE(3)));
    assertTrue(spec.isFullyBypassedTable(TABLE(3)));
  }

  @Test
  public void excludedTableNamesPatterns() {
    mBuilder.bypass()
        .exclude()
        .addTable(TABLE(0))
        .addTable(Pattern.compile(TABLE("[12]")));

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(TABLE(0)));
    assertFalse(spec.isBypassedTable(TABLE(1)));
    assertFalse(spec.isBypassedTable(TABLE(2)));
    assertFalse(spec.isFullyBypassedTable(TABLE(0)));
    assertFalse(spec.isFullyBypassedTable(TABLE(1)));
    assertFalse(spec.isFullyBypassedTable(TABLE(2)));
    assertTrue(spec.isBypassedTable(TABLE(3)));
    assertTrue(spec.isFullyBypassedTable(TABLE(3)));
  }

  @Test
  public void excludedPartitionsOfIncludedTable() {
    mBuilder.bypass()
        .exclude()
        .addPartition(TABLE(1), PART(1));

    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedPartition(TABLE(1), PART(1)));
    assertTrue(spec.isBypassedPartition(TABLE(1), PART(2)));
  }

  @Test
  public void excludeEverythingIsIncludeNothing() {
    mBuilder.bypass().exclude().addTable(Pattern.compile(".*"));
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  @Test
  public void excludeNothingIsIncludeNothing() {
    mBuilder.bypass()
        .exclude().addTables(Collections.emptySet()).addTables(Collections.emptySet());
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isBypassedTable(ANY_TABLE));
  }

  /* Ignoring tests */
  @Test
  public void ignoredTables() {
    mBuilder.ignore()
        .include()
        .addTable(TABLE(1))
        .addTables(ImmutableSet.of(TABLE(2), TABLE(3)))
        .addTable(Pattern.compile(TABLE(4)))
        .addTables(ImmutableSet.of(
            Pattern.compile(TABLE(5)),
            Pattern.compile(TABLE(6))
        ));

    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(TABLE(1)));
    assertTrue(spec.isIgnoredTable(TABLE(2)));
    assertTrue(spec.isIgnoredTable(TABLE(3)));
    assertTrue(spec.isIgnoredTable(TABLE(4)));
    assertTrue(spec.isIgnoredTable(TABLE(5)));
    assertTrue(spec.isIgnoredTable(TABLE(6)));
    assertFalse(spec.isIgnoredTable(TABLE(7)));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass() {
    mBuilder.bypass().include().addTable("same_table");
    mBuilder.ignore().include().addTable("same_table");
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable("same_table"));
    assertFalse(spec.isBypassedTable("same_table"));
  }

  @Test
  public void ignoreTakesPrecedenceOverBypass2() {
    mBuilder.bypass().include().addTable(TABLE(1));
    mBuilder.ignore().exclude().addTable(TABLE(2));
    UdbAttachSpec spec = mBuilder.build();
    assertTrue(spec.isIgnoredTable(TABLE(1)));
    assertFalse(spec.isBypassedTable(TABLE(1)));
  }

  @Test
  public void ignoreNone() {
    mBuilder.ignore().include().addTables(ImmutableSet.of());
    mBuilder.ignore().include().addTables(ImmutableSet.of());
    UdbAttachSpec spec = mBuilder.build();
    assertFalse(spec.isIgnoredTable(ANY_TABLE));
  }
}
