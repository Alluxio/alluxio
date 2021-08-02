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

import static alluxio.master.table.DbConfig.AbstractSpecObject.FIELD_TYPE;
import static alluxio.master.table.DbConfig.BypassTablesSpec;
import static alluxio.master.table.DbConfig.IncludeExcludeObject;
import static alluxio.master.table.DbConfig.NameObject;
import static alluxio.master.table.DbConfig.PartitionSpecObject;
import static alluxio.master.table.DbConfig.RegexObject;
import static alluxio.master.table.DbConfig.TablePartitionSpecObject.TYPE_PARTITION_SPEC;
import static alluxio.master.table.DbConfig.TablePartitionSpecObject.TYPE_NAME;
import static alluxio.master.table.DbConfig.TablePartitionSpecObject.TYPE_REGEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.master.table.DbConfig.NameOrRegexObject;
import alluxio.master.table.DbConfig.TablePartitionSpecObject;
import alluxio.master.table.DbConfig.TablePartitionSpecObject.Type;
import alluxio.master.table.DbConfig.TablesEntry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DbConfigTest {
  private ObjectMapper mMapper;

  @Before
  public void before() {
    mMapper = new ObjectMapper();
  }

  private static ObjectNode newObjectNode() {
    return new ObjectNode(new JsonNodeFactory(false));
  }

  private static ObjectNode name(String name) {
    return newObjectNode().put(FIELD_TYPE, TYPE_NAME).put(NameObject.FIELD_NAME, name);
  }

  private static ObjectNode regex(String regex) {
    return newObjectNode().put(FIELD_TYPE, TYPE_REGEX).put(RegexObject.FIELD_REGEX, regex);
  }

  private static ObjectNode partitionsOf(String tableName, ObjectNode partitions) {
    return newObjectNode().put(FIELD_TYPE, TYPE_PARTITION_SPEC)
        .put(PartitionSpecObject.FIELD_TABLE, tableName)
        .set(PartitionSpecObject.FIELD_PARTITIONS, partitions);
  }

  private static ObjectNode include(ObjectNode... nodes) {
    ArrayNode array = new ArrayNode(new JsonNodeFactory(false));
    Arrays.stream(nodes).forEach(array::add);
    return newObjectNode().set(IncludeExcludeObject.FIELD_INCLUDE, array);
  }

  private static ObjectNode exclude(ObjectNode... nodes) {
    ArrayNode array = new ArrayNode(new JsonNodeFactory(false));
    Arrays.stream(nodes).forEach(array::add);
    return newObjectNode().set(IncludeExcludeObject.FIELD_EXCLUDE, array);
  }

  private static ObjectNode tables(ObjectNode contained) {
    return newObjectNode().set(TablesEntry.FIELD_TABLES, contained);
  }

  /* NameObject and RegexObject tests */
  @Test
  public void simpleName() throws Exception {
    NameObject entry = mMapper.readValue(name("table1").toString(), NameObject.class);
    assertEquals("table1", entry.getName());
  }

  @Test
  public void regexPattern() throws Exception {
    RegexObject entry = mMapper.readValue(regex("^table\\d$").toString(), RegexObject.class);
    assertEquals("^table\\d$", entry.getPattern().pattern());
  }

  @Test
  public void rejectUnknownKey() throws Exception {
    assertThrows(UnrecognizedPropertyException.class,
        () -> mMapper.readValue(name("table").put("unknown_key", "some_value").toString(),
          NameObject.class));
    assertThrows(UnrecognizedPropertyException.class,
        () -> mMapper.readValue(regex("table").put("unknown_key", "some_value").toString(),
            RegexObject.class));
  }

  @Test
  public void rejectBadPattern() throws Exception {
    Exception e = assertThrows(ValueInstantiationException.class,
        () -> mMapper.readValue(regex("unclosed parenthesis (").toString(), RegexObject.class));
    assertTrue(e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectEmptyName() throws Exception {
    Exception e = assertThrows(ValueInstantiationException.class,
        () -> mMapper.readValue(name("").toString(), NameObject.class));
    assertTrue(e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectNullValues() throws Exception {
    // missing NameObject.FIELD_NAME or RegexObject.FIELD_REGEX fields leads to null values
    ObjectNode nameNode = newObjectNode()
        .put(FIELD_TYPE, TYPE_NAME);
    Exception e = assertThrows(ValueInstantiationException.class,
        () -> mMapper.readValue(nameNode.toString(), NameObject.class));
    assertTrue(e.getCause() instanceof IllegalArgumentException);

    ObjectNode regexNode = newObjectNode()
        .put(FIELD_TYPE, TYPE_REGEX);
    e = assertThrows(ValueInstantiationException.class,
        () -> mMapper.readValue(regexNode.toString(), RegexObject.class));
    assertTrue(e.getCause() instanceof IllegalArgumentException);
  }

  /* PartitionSpecObject tests */
  @Test
  public void includedPartitions() throws Exception {
    ObjectNode partitionNode =
        partitionsOf("table2", include(name("t2p1"), name("t2p2")));
    PartitionSpecObject entry =
        mMapper.readValue(partitionNode.toString(), PartitionSpecObject.class);
    assertEquals("table2", entry.getTableName());
    assertEquals(
        ImmutableSet.of(new NameObject("t2p1"), new NameObject("t2p2")),
        entry.getPartitions().getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getPartitions().getExcludedEntries());
  }

  @Test
  public void excludedPartitions() throws Exception {
    ObjectNode partitionNode =
        partitionsOf("table2", exclude(name("t2p1")));
    PartitionSpecObject entry =
        mMapper.readValue(partitionNode.toString(), PartitionSpecObject.class);
    assertEquals("table2", entry.getTableName());
    assertEquals(
        ImmutableSet.of(new NameObject("t2p1")),
        entry.getPartitions().getExcludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getPartitions().getIncludedEntries());
  }

  @Test
  public void rejectMissingPartitions() throws Exception {
    ObjectNode partitions = newObjectNode()
        .put(FIELD_TYPE, TYPE_PARTITION_SPEC)
        .put(PartitionSpecObject.FIELD_TABLE, "table2");
    Exception e = assertThrows(ValueInstantiationException.class,
        () -> mMapper.readValue(partitions.toString(), PartitionSpecObject.class));
    assertTrue(e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void equalityRegardlessOfPartitions() throws Exception {
    ObjectNode partitionNode = partitionsOf("table4", include(name("p1")));
    PartitionSpecObject entry1 =
        mMapper.readValue(partitionNode.toString(), PartitionSpecObject.class);

    partitionNode = partitionsOf("table4", include(name("p2")));
    PartitionSpecObject entry2 =
        mMapper.readValue(partitionNode.toString(), PartitionSpecObject.class);

    assertEquals(entry1, entry2);
    assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  /* IncludeExcludeObject tests */
  @Test
  public void rejectIncludeAndExcludeAtSameTime() throws Exception {
    ObjectNode node = include(name("table1"));
    node.set(IncludeExcludeObject.FIELD_EXCLUDE, node.arrayNode().add(name("table2")));
    Exception e =
        assertThrows(ValueInstantiationException.class, () -> mMapper.readValue(node.toString(),
        new TypeReference<IncludeExcludeObject<NameObject, NameObject>>() {}));
    assertTrue(e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void includeOnlyNameEntry() throws Exception {
    IncludeExcludeObject<NameObject, NameObject> list =
        mMapper.readValue(include(name("table1")).toString(),
            new TypeReference<IncludeExcludeObject<NameObject, NameObject>>() {});
    assertEquals(ImmutableSet.of(new NameObject("table1")), list.getIncludedEntries());
    assertEquals(ImmutableSet.of(), list.getExcludedEntries());
  }

  @Test
  public void includeNameRegexEntry() throws Exception {
    IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject> list =
        mMapper.readValue(include(name("table1"), regex("^table[2]$")).toString(),
            new TypeReference<IncludeExcludeObject<NameOrRegexObject, NameOrRegexObject>>() {});
    assertEquals(
        ImmutableSet.of(
            new NameObject("table1"),
            new RegexObject("^table[2]$")
        ),
        list.getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), list.getExcludedEntries());
  }

  @Test
  public void excludeOnlyNameEntry() throws Exception {
    IncludeExcludeObject<NameObject, NameObject> list =
        mMapper.readValue(exclude(name("table1")).toString(),
            new TypeReference<IncludeExcludeObject<NameObject, NameObject>>() {});
    assertEquals(ImmutableSet.of(new NameObject("table1")), list.getExcludedEntries());
    assertEquals(ImmutableSet.of(), list.getIncludedEntries());
  }

  @Test
  public void emptyListOfTables() throws Exception {
    IncludeExcludeObject<NameObject, NameObject> entry =
        mMapper.readValue(include().toString(),
            new TypeReference<IncludeExcludeObject<NameObject, NameObject>>() {});
    assertEquals(ImmutableSet.of(), entry.getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getExcludedEntries());
  }

  /* TablesEntry tests */
  @Test
  public void nullConstructor() throws Exception {
    TablesEntry entry2 = new TablesEntry(null);
    assertEquals(ImmutableSet.of(), entry2.getTables().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry2.getTables().getExcludedEntries());
  }

  @Test
  public void includeExcludeList() throws Exception {
    TablesEntry<NameObject, NameObject> entry =
        mMapper.readValue(tables(include(name("table1"))).toString(),
            new TypeReference<TablesEntry<NameObject, NameObject>>() {});
    assertEquals(
        ImmutableSet.of(new NameObject("table1")),
        entry.getTables().getIncludedEntries()
    );
    assertEquals(ImmutableSet.of(), entry.getTables().getExcludedEntries());
  }

  @Test
  public void bypassTablesEntry() throws Exception {
    BypassTablesSpec entry =
        mMapper.readValue(
            tables(include(partitionsOf("table1", include(name("p1"))))).toString(),
            new TypeReference<BypassTablesSpec>() {});
    assertEquals(
        ImmutableSet.of(new PartitionSpecObject("table1",
            new IncludeExcludeObject<>(ImmutableSet.of(new NameObject("p1")), null))
        ),
        entry.getTables().getIncludedEntries());
    assertEquals(ImmutableSet.of(), entry.getTables().getExcludedEntries());
  }

  /* TablePartitionSpecObject tests */
  @Test
  public void polymorphicDeserialization() throws Exception {
    TablePartitionSpecObject nameObject =
        mMapper.readValue(name("table1").toString(), TablePartitionSpecObject.class);
    assertEquals(Type.NAME, nameObject.getType());
    assertEquals("table1", ((NameObject) nameObject).getName());

    TablePartitionSpecObject regexObject =
        mMapper.readValue(regex("table[0-9]").toString(), TablePartitionSpecObject.class);
    assertEquals(Type.REGEX, regexObject.getType());
    assertEquals("table[0-9]", ((RegexObject) regexObject).getPattern().pattern());

    TablePartitionSpecObject partitionSpecObject =
        mMapper.readValue(partitionsOf("table2", include(name("p1"))).toString(),
            TablePartitionSpecObject.class);
    assertEquals(Type.PARTITION_SPEC, partitionSpecObject.getType());
    assertEquals("table2", ((PartitionSpecObject) partitionSpecObject).getTableName());
    assertEquals(ImmutableSet.of(new NameObject("p1")),
        ((PartitionSpecObject) partitionSpecObject).getPartitions().getIncludedEntries());
  }

  @Test
  public void emptyConfig() throws Exception {
    List<String> sources = ImmutableList.of(
        // "" empty string will cause MismatchedInputException
        "{}",
        "{\"bypass\": null}",
        "{\"ignore\": null}",
        "{\"bypass\": null, \"ignore\": null}"
    );
    for (String source : sources) {
      DbConfig config = mMapper.readValue(source, DbConfig.class);
      assertEquals(DbConfig.empty(), config);
    }
  }

  // Todo(bowen): add test to cover DbConfig.getUdbAttachOptions
}
