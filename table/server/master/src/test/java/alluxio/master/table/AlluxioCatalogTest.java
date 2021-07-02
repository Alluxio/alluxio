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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.StringColumnStatsData;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.table.common.Layout;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.table.common.transform.action.TransformActionUtils;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabaseRegistry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class AlluxioCatalogTest {
  private static final TransformDefinition TRANSFORM_DEFINITION =
      TransformDefinition.parse("file.count.max=100");

  private AlluxioCatalog mCatalog;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() {
    mCatalog = new AlluxioCatalog();
    TestDatabase.reset();
  }

  @Test
  public void attachDb() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 2, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
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
    TestDatabase.genTable(1, 2, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    assertEquals(1, mCatalog.getAllDatabases().size());
    assertTrue(mCatalog.detachDatabase(NoopJournalContext.INSTANCE, dbName));
    assertEquals(0, mCatalog.getAllDatabases().size());
  }

  @Test
  public void getDb() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 2, false);

    try {
      mCatalog.getDatabase(dbName);
      fail();
    } catch (IOException e) {
      assertEquals("Database " + dbName + " does not exist", e.getMessage());
    }

    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    assertEquals(dbName, mCatalog.getDatabase(dbName).getDbName());
    assertEquals(TestDatabase.sTestDbInfo.getComment(),
        mCatalog.getDatabase(dbName).getComment());
    assertEquals(TestDatabase.sTestDbInfo.getLocation(),
        mCatalog.getDatabase(dbName).getLocation());
    assertEquals(TestDatabase.sTestDbInfo.getOwnerName(),
        mCatalog.getDatabase(dbName).getOwnerName());
    assertEquals(TestDatabase.sTestDbInfo.getOwnerType(),
        mCatalog.getDatabase(dbName).getOwnerType());
    assertEquals(TestDatabase.sTestDbInfo.getParameters(),
        mCatalog.getDatabase(dbName).getParameterMap());
  }

  @Test
  public void testGetAllDatabase() throws Exception {
    addMockDbs();
    assertEquals(2, mCatalog.getAllDatabases().size());
    TestDatabase.genTable(1, 2, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, "testdb",
        Collections.emptyMap(), false);
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
    addTableToDb(db, Table.create(db, tbl, null));
    addDbToCatalog(db);
    assertEquals(1, mCatalog.getTable("test", "test").getPartitions().size());
  }

  @Test
  public void testGetPartitionPartitonedUdbTable() throws Exception {
    Schema s = schemaFromColNames("c1", "c2", "c3");
    // setup
    UdbTable tbl = createMockPartitionedUdbTable("test", s);
    Database db = createMockDatabase("noop", "test", Collections.emptyList());
    addTableToDb(db, Table.create(db, tbl, null));
    addDbToCatalog(db);
    assertEquals(2, mCatalog.getTable("test", "test").getPartitions().size());
  }

  @Test
  public void testGetPartitionColumnStats() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 2, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    // single partition
    assertEquals(1, mCatalog.getPartitionColumnStatistics(dbName,
        TestDatabase.getTableName(0),
        Arrays.asList(TestUdbTable.getPartName(0)), Arrays.asList("col2")).size());

    // multiple partitions
    assertEquals(2, mCatalog.getPartitionColumnStatistics(dbName,
        TestDatabase.getTableName(0),
        Arrays.asList(TestUdbTable.getPartName(0), TestUdbTable.getPartName(1)),
        Arrays.asList("col2")).size());

    // unknown column
    assertEquals(2, mCatalog.getPartitionColumnStatistics(dbName,
        TestDatabase.getTableName(0),
        Arrays.asList(TestUdbTable.getPartName(0), TestUdbTable.getPartName(1)),
        Arrays.asList("col3")).size());

    // unknown partition
    assertEquals(0, mCatalog.getPartitionColumnStatistics(dbName,
        TestDatabase.getTableName(0),
        Arrays.asList(TestUdbTable.getPartName(3)),
        Arrays.asList("col2")).size());
  }

  @Test
  public void testGetColumnStats() throws Exception {
    Schema s = schemaFromColNames("c1", "c2", "c3");
    // setup
    // Why does this API seem so counter intuitive?
    UdbTable tbl = createMockUdbTable("test", s);
    Database db = createMockDatabase("noop", "test", Collections.emptyList());
    addTableToDb(db, Table.create(db, tbl, null));
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

  @Test
  public void getTransformPlanForNonExistingDatabase() throws IOException {
    String dbName = "doesnotexist";
    mException.expect(NotFoundException.class);
    mException.expectMessage(ExceptionMessage.DATABASE_DOES_NOT_EXIST.getMessage(dbName));
    mCatalog.getTransformPlan(dbName, "table", TRANSFORM_DEFINITION);
  }

  @Test
  public void getTransformPlanForNonExistingTable() throws IOException {
    String dbName = "existingdb";
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    assertEquals(1, mCatalog.getAllDatabases().size());
    assertEquals(0, mCatalog.getAllTables(dbName).size());
    String tableName = "doesnotexist";
    mException.expect(NotFoundException.class);
    mException.expectMessage(ExceptionMessage.TABLE_DOES_NOT_EXIST.getMessage(tableName, dbName));
    mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);
  }

  @Test
  public void getTransformPlan() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 1, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    assertEquals(1, mCatalog.getAllDatabases().size());
    assertEquals(1, mCatalog.getAllTables(dbName).size());
    String tableName = TestDatabase.getTableName(0);
    // When generating transform plan, the authority of the output path
    // will be determined based on this hostname configuration.
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    List<TransformPlan> plans = mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);
    assertEquals(1, plans.size());
    Table table = mCatalog.getTable(dbName, tableName);
    assertEquals(1, table.getPartitions().size());
    assertEquals(table.getPartitions().get(0).getLayout(), plans.get(0).getBaseLayout());
  }

  @Test
  public void getTransformPlanOutputUri() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 1, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    String tableName = TestDatabase.getTableName(0);
    Table table = mCatalog.getTable(dbName, tableName);

    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT, "8080");
    List<TransformPlan> plans = mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);
    assertEquals("alluxio://localhost:8080/",
        plans.get(0).getTransformedLayout().getLocation().getRootPath());

    ServerConfiguration.set(PropertyKey.MASTER_RPC_ADDRESSES, "host1:1,host2:2");
    plans = mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);
    assertEquals("alluxio://host1:1,host2:2/",
        plans.get(0).getTransformedLayout().getLocation().getRootPath());

    ServerConfiguration.set(PropertyKey.ZOOKEEPER_ENABLED, "true");
    ServerConfiguration.set(PropertyKey.ZOOKEEPER_ADDRESS, "host:1000");
    plans = mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);
    assertEquals("alluxio://zk@host:1000/",
        plans.get(0).getTransformedLayout().getLocation().getRootPath());
  }

  @Test
  public void getTransformPlanTransformedLayout() throws Exception {
    String dbName = "testdb";
    TestDatabase.genTable(1, 1, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    String tableName = TestDatabase.getTableName(0);

    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");

    final TransformDefinition transformDefinition =
        TransformDefinition.parse("file.count.max=100;file.parquet.compression=uncompressed");

    List<TransformPlan> plans = mCatalog.getTransformPlan(dbName, tableName, transformDefinition);
    assertEquals(1, plans.size());
    alluxio.job.plan.transform.PartitionInfo transformedPartitionInfo =
        TransformActionUtils.generatePartitionInfo(plans.get(0).getTransformedLayout());
    assertEquals("uncompressed",
        transformedPartitionInfo.getSerdeProperties().get("file.parquet.compression"));
  }

  @Test
  public void completeTransformNonExistingDatabase() throws IOException {
    String dbName = "doesnotexist";
    mException.expect(NotFoundException.class);
    mException.expectMessage(ExceptionMessage.DATABASE_DOES_NOT_EXIST.getMessage(dbName));
    mCatalog.completeTransformTable(NoopJournalContext.INSTANCE, dbName, "table",
        TRANSFORM_DEFINITION.getDefinition(), Collections.emptyMap());
  }

  @Test
  public void completeTransformNonExistingTable() throws IOException {
    String dbName = "existingdb";
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    assertEquals(1, mCatalog.getAllDatabases().size());
    assertEquals(0, mCatalog.getAllTables(dbName).size());
    String tableName = "doesnotexist";
    mException.expect(NotFoundException.class);
    mException.expectMessage(ExceptionMessage.TABLE_DOES_NOT_EXIST.getMessage(tableName, dbName));
    mCatalog.completeTransformTable(NoopJournalContext.INSTANCE, dbName, tableName,
        TRANSFORM_DEFINITION.getDefinition(), Collections.emptyMap());
  }

  @Test
  public void completeTransformTable() throws IOException {
    String dbName = "testdb";
    TestDatabase.genTable(1, 10, false);
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE,
        TestUdbFactory.TYPE, "connect_URI", TestDatabase.TEST_UDB_NAME, dbName,
        Collections.emptyMap(), false);
    String tableName = TestDatabase.getTableName(0);

    Table table = mCatalog.getTable(dbName, tableName);
    table.getPartitions().forEach(partition ->
        assertFalse(partition.isTransformed(TRANSFORM_DEFINITION.getDefinition())));

    // When generating transform plan, the authority of the output path
    // will be determined based on this hostname configuration.
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    List<TransformPlan> plans = mCatalog.getTransformPlan(dbName, tableName, TRANSFORM_DEFINITION);

    Map<String, Layout> transformedLayouts = Maps.newHashMapWithExpectedSize(plans.size());
    plans.forEach(plan ->
        transformedLayouts.put(plan.getBaseLayout().getSpec(), plan.getTransformedLayout()));
    mCatalog.completeTransformTable(NoopJournalContext.INSTANCE, dbName, tableName,
        TRANSFORM_DEFINITION.getDefinition(), transformedLayouts);

    table.getPartitions().forEach(partition -> {
      assertTrue(partition.isTransformed(TRANSFORM_DEFINITION.getDefinition()));
      assertEquals(transformedLayouts.get(partition.getSpec()), partition.getLayout());
    });
  }

  @Test
  public void parallelSync() throws Exception {
    // this should be larger than numThreads, to guarantee all threads are utilized
    int numTables = 200;
    TestDatabase.genTable(numTables, 2, false);

    testParallelSyncInternal("8", 8);
    testParallelSyncInternal("16", 16);
    testParallelSyncInternal("1", 1);
    testParallelSyncInternal("0", CatalogProperty.DEFAULT_DB_SYNC_THREADS);
    testParallelSyncInternal("-1", CatalogProperty.DEFAULT_DB_SYNC_THREADS);
    testParallelSyncInternal("", CatalogProperty.DEFAULT_DB_SYNC_THREADS);
    testParallelSyncInternal("not an int", CatalogProperty.DEFAULT_DB_SYNC_THREADS);
  }

  /**
   * @param syncThreads the string value for the sync threads parameter. remains unset if null
   * @param expectedThreadsUsed the expected number of threads used for the sync
   */
  private void testParallelSyncInternal(String syncThreads, int expectedThreadsUsed)
      throws Exception {
    Map<String, String> attachConf = Collections.emptyMap();
    if (syncThreads != null) {
      attachConf = ImmutableMap.of(CatalogProperty.DB_SYNC_THREADS.getName(), syncThreads);
    }
    // verify thread count for attach
    assertTrue(TestDatabase.getTableThreadNames().isEmpty());
    mCatalog.attachDatabase(NoopJournalContext.INSTANCE, TestUdbFactory.TYPE, "connect_URI",
        TestDatabase.TEST_UDB_NAME, TestDatabase.TEST_UDB_NAME, attachConf, false);
    Set<String> threadNames = TestDatabase.getTableThreadNames();
    assertEquals("unexpected # threads used for attach for config value: " + syncThreads,
        expectedThreadsUsed, threadNames.size());

    // verify thread count for sync
    TestDatabase.resetGetTableThreadNames();
    assertTrue(TestDatabase.getTableThreadNames().isEmpty());
    mCatalog.syncDatabase(NoopJournalContext.INSTANCE, TestDatabase.TEST_UDB_NAME);
    threadNames = TestDatabase.getTableThreadNames();
    assertEquals("unexpected # threads used for sync for config value: " + syncThreads,
        expectedThreadsUsed, threadNames.size());

    // reset the state by detaching and resetting the thread name set
    mCatalog.detachDatabase(NoopJournalContext.INSTANCE, TestDatabase.TEST_UDB_NAME);
    TestDatabase.resetGetTableThreadNames();
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

  private void addTableToDb(Database db, Table table) {
    Map<String, Table> dbTables = Whitebox.getInternalState(db, "mTables");
    dbTables.put(table.getName(), table);
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
    tables.forEach(table -> addTableToDb(db, table));
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
