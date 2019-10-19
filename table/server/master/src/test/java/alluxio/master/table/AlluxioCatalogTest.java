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
import static org.mockito.Mockito.when;

import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.StringColumnStatsData;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabaseRegistry;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class AlluxioCatalogTest {

  private AlluxioCatalog mCatalog;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() {
    mCatalog = new AlluxioCatalog();
  }

  @Test
  public void attachDb() throws Exception {
    String dbName = "testdb";
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        NoopUdbFactory.TYPE, dbName,
        Collections.emptyMap());
    List<String> dbs = mCatalog.getAllDatabases();
    assertEquals(1, dbs.size());
    assertEquals(dbName, dbs.get(0));
  }

  @Test
  public void detachNonExistingDb() throws Exception {
    mException.expect(IOException.class);
    mCatalog.detachDatabase(NoopJournalContext.INSTANCE, "testDb");
  }

  @Test
  public void detachDb() throws Exception {
    String dbName = "testdb";
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        NoopUdbFactory.TYPE, dbName,
        Collections.emptyMap());
    assertEquals(1, mCatalog.getAllDatabases().size());
    assertTrue(mCatalog.detachDatabase(NoopJournalContext.INSTANCE, dbName));
    assertEquals(0, mCatalog.getAllDatabases().size());
  }

  @Test
  public void testGetAllDatabase() throws Exception {
    addMockDbs();
    assertEquals(2, mCatalog.getAllDatabases().size());
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        NoopUdbFactory.TYPE, "noop",
        Collections.emptyMap());
    assertEquals(3, mCatalog.getAllDatabases().size());
  }

  @Test
  public void testGetAllTablesNotFound() throws Exception {
    mException.expect(NotFoundException.class);
    mCatalog.getAllTables("dbs");
  }

  @Test
  public void testGetAllTablesFound() throws Exception {
    addMockDbs();
    assertTrue(mCatalog.getAllTables("db2").contains("1"));
    assertTrue(mCatalog.getAllTables("db2").contains("2"));
    assertTrue(mCatalog.getAllTables("db2").contains("3"));
    assertTrue(mCatalog.getAllTables("db2").contains("4"));
  }

  @Test
  public void testGetNotExistentTable() throws Exception {
    addMockDbs();
    mException.expect(NotFoundException.class);
    mCatalog.getTable("db1", "noop");
  }

  @Test
  public void testGetExistingTables() throws Exception {
    addMockDbs();
    assertEquals("1", mCatalog.getTable("db2", "1").getName());
    assertEquals("2", mCatalog.getTable("db2", "2").getName());
    assertEquals("3", mCatalog.getTable("db2", "3").getName());
    assertEquals("4", mCatalog.getTable("db2", "4").getName());
    mException.expect(NotFoundException.class);
    mCatalog.getTable("db2", "5");
  }

  @Test
  public void testGetPartitionUnpartitonedUdbTable() throws Exception {
    Schema s = schemaFromColNames("c1", "c2", "c3");
    // setup
    UdbTable tbl = createMockUdbTable("test", s);
    Database db = createMockDatabase("noop", "test", Collections.emptyList());
    db.addTable(tbl.getName(), Table.create(db, tbl));
    addDbToCatalog(db);
    assertEquals(1, mCatalog.getTable("test", "test").getPartitions().size());
  }

  @Test
  public void testGetPartitionPartitonedUdbTable() throws Exception {
    Schema s = schemaFromColNames("c1", "c2", "c3");
    // setup
    UdbTable tbl = createMockPartitionedUdbTable("test", s);
    Database db = createMockDatabase("noop", "test", Collections.emptyList());
    db.addTable(tbl.getName(), Table.create(db, tbl));
    addDbToCatalog(db);
    assertEquals(2, mCatalog.getTable("test", "test").getPartitions().size());
  }

  @Test
  public void testGetColumnStats() throws Exception {
    Schema s = schemaFromColNames("c1", "c2", "c3");
    // setup
    // Why does this API seem so counter intuitive?
    UdbTable tbl = createMockUdbTable("test", s);
    Database db = createMockDatabase("noop", "test", Collections.emptyList());
    db.addTable(tbl.getName(), Table.create(db, tbl));
    addDbToCatalog(db);

    // basic, filter on each col
    assertEquals(1,
        mCatalog.getTableColumnStatistics("test", "test", Lists.newArrayList("c1")).size());
    assertEquals(1,
        mCatalog.getTableColumnStatistics("test", "test", Lists.newArrayList("c2")).size());
    assertEquals(1,
        mCatalog.getTableColumnStatistics("test", "test", Lists.newArrayList("c3")).size());

    // try two
    assertEquals(2,
        mCatalog.getTableColumnStatistics("test", "test", Lists.newArrayList("c1", "c2")).size());
    // flip order
    assertEquals(2,
        mCatalog.getTableColumnStatistics("test", "test", Lists.newArrayList("c2", "c1")).size());

    // non existing
    assertEquals(0, mCatalog.getTableColumnStatistics("test", "test",
        Lists.newArrayList("doesnotexist")).size());

    // empty
    assertEquals(0, mCatalog.getTableColumnStatistics("test", "test",
        Lists.newArrayList()).size());
  }

  /**
   * Add mock database of name "db1" and "db2" to the catalog
   *
   * db1 has no tables.
   *
   * db2 has 4 mock tables
   *
   * @return a map of db names to database objects
   */
  private Map<String, Database> addMockDbs() {
    Database db1 = createMockDatabase("noop", "db1", Collections.emptyList());
    List<Table> tables = Lists.newArrayList(1, 2, 3, 4).stream().map(i -> {
      Table t = Mockito.mock(Table.class);
      when(t.getName()).thenReturn(Integer.toString(i));
      return t;
    })
        .collect(Collectors.toList());
    Database db2 = createMockDatabase("noop", "db2", tables);
    Map<String, Database> dbs = new HashMap<>();
    dbs.put("db1", db1);
    dbs.put("db2", db2);
    assertEquals(0, dbs.get("db1").getTables().size());
    assertEquals(4, dbs.get("db2").getTables().size());
    Whitebox.setInternalState(mCatalog, "mDBs", dbs);
    return dbs;
  }

  private Database createMockDatabase(String type, String name, Collection<Table> tables) {
    UdbContext udbCtx = Mockito.mock(UdbContext.class);
    when(udbCtx.getUdbRegistry()).thenReturn(Mockito.mock(UnderDatabaseRegistry.class));
    Database db = Database.create(
        Mockito.mock(CatalogContext.class),
        udbCtx,
        type,
        name,
        Collections.emptyMap()
    );
    tables.forEach(table -> db.addTable(table.getName(), table));
    return db;
  }

  private void addDbToCatalogWithTables(String dbName, Collection<Table> tables) {
    Database db = createMockDatabase("noop", dbName, tables);
    addDbToCatalog(db);
  }

  private void addDbToCatalog(Database db) {
    ((Map<String, Database>) Whitebox.getInternalState(mCatalog, "mDBs")).put(db.getName(), db);
  }

  UdbTable createMockPartitionedUdbTable(String name, Schema schema) throws IOException {
    UdbPartition partition = Mockito.mock(UdbPartition.class);
    when(partition.getSpec()).thenReturn(name);
    when(partition.getLayout()).thenReturn(new HiveLayout(PartitionInfo.getDefaultInstance(),
        Collections.emptyList()));
    UdbTable tbl = Mockito.mock(UdbTable.class);
    when(tbl.getName()).thenReturn(name);
    when(tbl.getSchema()).thenReturn(schema);
    when(tbl.getStatistics()).thenReturn(createRandomStatsForSchema(schema));
    when(tbl.getPartitions()).thenReturn(Arrays.asList(partition, partition));
    when(tbl.getPartitionCols()).thenReturn(Arrays.asList(FieldSchema.getDefaultInstance()));
    when(tbl.getLayout()).thenReturn(new HiveLayout(PartitionInfo.getDefaultInstance(),
        Collections.emptyList()).toProto());
    return tbl;
  }

  UdbTable createMockUdbTable(String name, Schema schema) throws IOException {
    UdbPartition partition = Mockito.mock(UdbPartition.class);
    when(partition.getSpec()).thenReturn(name);
    when(partition.getLayout()).thenReturn(new HiveLayout(PartitionInfo.getDefaultInstance(),
        Collections.emptyList()));
    UdbTable tbl = Mockito.mock(UdbTable.class);
    when(tbl.getName()).thenReturn(name);
    when(tbl.getSchema()).thenReturn(schema);
    when(tbl.getStatistics()).thenReturn(createRandomStatsForSchema(schema));
    when(tbl.getPartitions()).thenReturn(Arrays.asList(partition));
    when(tbl.getPartitionCols()).thenReturn(Collections.emptyList());
    when(tbl.getLayout()).thenReturn(new HiveLayout(PartitionInfo.getDefaultInstance(),
        Collections.emptyList()).toProto());
    return tbl;
  }

  Schema schemaFromColNames(String... names) {
    Schema.Builder s = Schema.newBuilder();
    for (int i = 0; i < names.length; i++) {
      s.addCols(FieldSchema.newBuilder().setName(names[i]).setType("string").build());
    }
    return s.build();
  }

  List<ColumnStatisticsInfo> createRandomStatsForSchema(Schema s) {
    return s.getColsList().stream().map(f -> {
      if (!f.getType().equals("string")) {
        throw new RuntimeException("can only generate random stats for string columns");
      }
      return ColumnStatisticsInfo.newBuilder()
          .setColName(f.getName())
          .setColType(f.getType())
          .setData(
              ColumnStatisticsData.newBuilder()
                  .setStringStats(
                      StringColumnStatsData.newBuilder()
                          .setAvgColLen(ThreadLocalRandom.current().nextInt() % 1000)
                          .setMaxColLen((ThreadLocalRandom.current().nextInt() % 1000) + 750)
                          .setNumNulls(ThreadLocalRandom.current().nextInt() % 100)
                          .build()
                  )
                  .build()
          )
          .build();
    }).collect(Collectors.toList());
  }
}
