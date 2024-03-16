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

package alluxio.table.under.hive;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.client.file.DelegatingFileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.table.common.udb.UdbBypassSpec;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbProperty;
import alluxio.table.under.hive.util.TestHiveClientPool;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HiveDatabaseTest {

  private static final String DB_NAME = "test";
  private static final String DB_TYPE = "hive";
  private static final String CONNECTION_URI = "thrift://not_running:9083";
  private static final String DB_LOCATION = PathUtils.concatPath("hdfs://node1/", DB_NAME);
  private static final String DB_ALT_LOCATION = PathUtils.concatPath("hdfs://node2/", DB_NAME);
  private static final Map<String, String> CONF = new HashMap<>();
  private static final UdbBypassSpec EMPTY_BYPASS_SPEC = new UdbBypassSpec(ImmutableMap.of());

  @Rule
  public ExpectedException mExpection = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfiguration =
      new ConfigurationRule(
          ImmutableMap.of(PropertyKey.MASTER_HOSTNAME, "master",
              PropertyKey.MASTER_RPC_PORT, "10000"),
          ServerConfiguration.global());

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConf;
  private final MountRecordingFileSystem mFs = new MountRecordingFileSystem();
  private final IMetaStoreClient mMockedClient = Mockito.mock(IMetaStoreClient.class);
  private final MockedHiveClientPool mClientPool = new MockedHiveClientPool(mMockedClient);
  private final UdbContext mUdbContextWithFs =
      new UdbContext(null, mFs, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);

  @Before
  public void before() {
    mUdbContext =
        new UdbContext(null, null, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);
    mUdbConf = new UdbConfiguration(CONF);
  }

  @Test
  public void create() {
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, mUdbConf).getName());
  }

  @Test
  public void createEmptyName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, DB_TYPE, CONNECTION_URI, "", DB_NAME);
    assertEquals(DB_NAME,
        HiveDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, DB_TYPE, CONNECTION_URI, null, DB_NAME);
    assertEquals(DB_NAME,
        HiveDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createEmptyConnectionUri() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext = new UdbContext(null, null, DB_TYPE, "", DB_NAME, DB_NAME);
    assertEquals(DB_NAME, HiveDatabase.create(udbContext,
        new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullConnectionUri() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext = new UdbContext(null, null, DB_TYPE, null, DB_NAME, DB_NAME);
    assertEquals(DB_NAME, HiveDatabase.create(udbContext,
        new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createWithProps() {
    Map<String, String> props = ImmutableMap.of(
        "prop1", "value1",
        "prop2", "value2"
    );
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, new UdbConfiguration(props)).getName());
  }

  @Test
  public void mountTablesOnly() throws Exception {
    Map<TestTable, List<TestPartition>> tables = ImmutableList.of("table1", "table2").stream()
        .map((name) -> TestTable.createWithNoPartitions(name, DB_LOCATION))
        .collect(ImmutableMap.toImmutableMap((table) -> table, (table) -> ImmutableList.of()));
    UdbConfiguration udbConf = getUdbConfigWithGroupMountEnabled(false);
    createDatabaseAndMountTables(tables, udbConf);

    Map<AlluxioURI, AlluxioURI> expectedMountPoints = tables.keySet().stream().collect(
        ImmutableMap.toImmutableMap(
            // key: the mount point of the table in Alluxio
            (table) -> mUdbContextWithFs.getTableLocation(table.getName()),
            // value: the ufs uri of the table
            (table) -> new AlluxioURI(table.getLocation())
    ));
    assertEquals(expectedMountPoints, mFs.getMountPoints());
  }

  @Test
  public void mountTableWithPartitions() throws Exception {
    TestTable table =
        TestTable.createWithPartitionKeys("table1", DB_LOCATION, ImmutableList.of("key1"));
    List<TestPartition> partitions = ImmutableList.of(
        TestPartition.createWithKeyValue(table.getLocation(),
            ImmutableList.of("key1"), ImmutableList.of("value1")),
        TestPartition.createWithKeyValue(table.getLocation(),
            ImmutableList.of("key1"), ImmutableList.of("value2")));
    UdbConfiguration udbConf = getUdbConfigWithGroupMountEnabled(false);
    createDatabaseAndMountTables(ImmutableMap.of(table, partitions), udbConf);

    AlluxioURI tableMountPoint = mUdbContextWithFs.getTableLocation(table.getName());
    // no separate mount point for the partitions since they're co-located with the parent table,
    // which is already mounted
    assertEquals(ImmutableMap.of(tableMountPoint, new AlluxioURI(table.getLocation())),
        mFs.getMountPoints());
  }

  @Test
  public void mountTableWithPartitionOfDifferentPrefix() throws Exception {
    TestTable table = TestTable.createWithDummyPartitionKeys("table1", DB_LOCATION);
    TestPartition partition =
        TestPartition.createWithDummyValues(PathUtils.concatPath(DB_ALT_LOCATION, table.getName()));
    UdbConfiguration udbConf =
        new UdbConfiguration(ImmutableMap.of(
            UdbProperty.GROUP_MOUNT_POINTS.getName(), "false",
            Property.ALLOW_DIFF_PART_LOC_PREFIX.getName(), "true"
        ));
    createDatabaseAndMountTables(ImmutableMap.of(table, ImmutableList.of(partition)), udbConf);

    AlluxioURI tableMountPoint = mUdbContextWithFs.getTableLocation(table.getName());
    AlluxioURI partitionMountPoint = tableMountPoint.join(partition.getName());
    assertEquals(
        ImmutableMap.of(
            tableMountPoint, new AlluxioURI(table.getLocation()),
            partitionMountPoint, new AlluxioURI(partition.getLocation())),
        mFs.getMountPoints());
  }

  @Test
  public void mountTableWithPartitionDisallowingDifferentPrefix() throws Exception {
    TestTable table = TestTable.createWithDummyPartitionKeys("table1", DB_LOCATION);
    TestPartition partition =
        TestPartition.createWithDummyValues(PathUtils.concatPath(DB_ALT_LOCATION, table.getName()));
    UdbConfiguration udbConf =
        new UdbConfiguration(ImmutableMap.of(
            UdbProperty.GROUP_MOUNT_POINTS.getName(), "false",
            Property.ALLOW_DIFF_PART_LOC_PREFIX.getName(), "false"
        ));
    createDatabaseAndMountTables(ImmutableMap.of(table, ImmutableList.of(partition)), udbConf);

    AlluxioURI tableMountPoint = mUdbContextWithFs.getTableLocation(table.getName());
    // the partition is ignored
    assertEquals(
        "The partition should be ignored since it has a different location prefix"
            + "and configuration disallowing such partitions",
        ImmutableMap.of(tableMountPoint, new AlluxioURI(table.getLocation())),
        mFs.getMountPoints());
  }

  @Test
  public void groupingMountPointsTablesOnly() throws Exception {
    Map<TestTable, List<TestPartition>> tables = ImmutableList.of("table1", "table2").stream()
        .map((name) -> TestTable.createWithNoPartitions(name, DB_LOCATION))
        .collect(ImmutableMap.toImmutableMap(Function.identity(), (table) -> ImmutableList.of()));
    UdbConfiguration udbConf = getUdbConfigWithGroupMountEnabled(true);
    createDatabaseAndMountTables(tables, udbConf);

    AlluxioURI dbUfsLocation = new AlluxioURI(DB_LOCATION);
    AlluxioURI dbMountPoint = mUdbContext.getFragmentLocation(dbUfsLocation);
    // all the tables share the same location prefix at the database level
    // so the mount points are grouped
    assertEquals(
        ImmutableMap.of(dbMountPoint, dbUfsLocation),
        mFs.getMountPoints());
  }

  @Test
  public void groupingMountPointsWithColocatedPartitions() throws Exception {
    TestTable table = TestTable.createWithDummyPartitionKeys("table1", DB_LOCATION);
    TestPartition partition = TestPartition.createColocatedWithDummyValues(table);
    UdbConfiguration udbConf = getUdbConfigWithGroupMountEnabled(true);
    createDatabaseAndMountTables(ImmutableMap.of(table, ImmutableList.of(partition)), udbConf);

    AlluxioURI dbUfsLocation = new AlluxioURI(DB_LOCATION);
    AlluxioURI dbMountPoint = mUdbContext.getFragmentLocation(dbUfsLocation);
    assertEquals(
        ImmutableMap.of(dbMountPoint, dbUfsLocation),
        mFs.getMountPoints());
  }

  @Test
  public void groupingMountPointsWithNonColocatedPartitions() throws Exception {
    TestTable table = TestTable.createWithDummyPartitionKeys("table1", DB_LOCATION);
    TestPartition colocatedPartition = TestPartition.createColocatedWithDummyValues(table);
    TestPartition nonColocatedPartition = TestPartition.createWithDummyValues(DB_ALT_LOCATION);
    UdbConfiguration udbConf =
        new UdbConfiguration(ImmutableMap.of(
            UdbProperty.GROUP_MOUNT_POINTS.getName(), "true",
            Property.ALLOW_DIFF_PART_LOC_PREFIX.getName(), "true"
        ));
    createDatabaseAndMountTables(
        ImmutableMap.of(table, ImmutableList.of(colocatedPartition, nonColocatedPartition)),
        udbConf);

    AlluxioURI dbUfsLocation = new AlluxioURI(DB_LOCATION);
    AlluxioURI dbMountPoint = mUdbContext.getFragmentLocation(dbUfsLocation);
    AlluxioURI partitionUfsLocation = new AlluxioURI(nonColocatedPartition.getLocation());
    AlluxioURI partitionMountPoint =
        mUdbContext.getTableLocation(table.getName()).join(nonColocatedPartition.getName());
    assertEquals(
        ImmutableMap.of(dbMountPoint, dbUfsLocation, partitionMountPoint, partitionUfsLocation),
        mFs.getMountPoints());
  }

  @Test
  public void groupingMountPointsWithNonColocatedTables() throws Exception {
    TestTable colocatedTable = TestTable.createWithNoPartitions("colocated", DB_LOCATION);
    TestTable nonColocatedTable = TestTable.createWithNoPartitions("nonColocated", DB_ALT_LOCATION);
    UdbConfiguration udbConf = getUdbConfigWithGroupMountEnabled(true);
    createDatabaseAndMountTables(
        ImmutableMap.of(colocatedTable, ImmutableList.of(), nonColocatedTable, ImmutableList.of()),
        udbConf);

    AlluxioURI dbUfsLocation = new AlluxioURI(DB_LOCATION);
    AlluxioURI dbMountPoint = mUdbContext.getFragmentLocation(dbUfsLocation);
    AlluxioURI nonColocatedLocation = new AlluxioURI(nonColocatedTable.getLocation());
    AlluxioURI nonColocatedMountPoint = mUdbContext.getTableLocation(nonColocatedTable.getName());
    assertEquals(
        ImmutableMap.of(dbMountPoint, dbUfsLocation, nonColocatedMountPoint, nonColocatedLocation),
        mFs.getMountPoints());
  }

  private static class TestTable {
    public static final List<String> DUMMY_PARTITION_KEYS = ImmutableList.of("col1", "col2");
    public static final List<String> DUMMY_PARTITION_VALUES = ImmutableList.of("val1", "val2");
    private final String mName;
    private final String mLocation;
    private final List<String> mPartitionKeys;

    private TestTable(String name, String location, List<String> partitionKeys) {
      mName = name;
      mLocation = location;
      mPartitionKeys = partitionKeys;
    }

    public static TestTable createWithPartitionKeys(String name,
                                                    String locationPrefix,
                                                    List<String> keys) {
      return new TestTable(name, PathUtils.concatPath(locationPrefix, name), keys);
    }

    public static TestTable createWithDummyPartitionKeys(String name, String locationPrefix) {
      return createWithPartitionKeys(name, locationPrefix, DUMMY_PARTITION_KEYS);
    }

    public static TestTable createWithNoPartitions(String name, String locationPrefix) {
      return createWithPartitionKeys(name, locationPrefix, ImmutableList.of());
    }

    public String getName() {
      return mName;
    }

    public String getLocation() {
      return mLocation;
    }

    public Table toHiveTable() {
      Table table = new Table();
      table.setTableName(mName);
      table.setDbName(DB_NAME);
      table.setPartitionKeys(
          mPartitionKeys.stream().map((key) -> {
            FieldSchema schema = new FieldSchema();
            schema.setName(key);
            return schema;
          }).collect(Collectors.toList())
      );
      StorageDescriptor tableSd = new StorageDescriptor();
      tableSd.setLocation(mLocation);
      table.setSd(tableSd);
      return table;
    }
  }

  private static class TestPartition {
    private final List<String> mKeys;
    private final List<String> mValues;
    private final String mPartitionName;
    private final String mLocation;

    private TestPartition(List<String> keys, List<String> values,
                          String partitionName, String location) {
      mKeys = keys;
      mValues = values;
      mPartitionName = partitionName;
      mLocation = location;
    }

    public static TestPartition createWithKeyValue(String locationPrefix,
                                                   List<String> keys, List<String> values) {
      Preconditions.checkArgument(keys.size() == values.size());
      String partName = FileUtils.makePartName(keys, values);
      return new TestPartition(keys, values, partName,
          PathUtils.concatPath(locationPrefix, partName));
    }

    public static TestPartition createWithDummyValues(String locationPrefix) {
      return createWithKeyValue(locationPrefix,
          TestTable.DUMMY_PARTITION_KEYS, TestTable.DUMMY_PARTITION_VALUES);
    }

    public static TestPartition createColocatedWithDummyValues(TestTable parentTable) {
      return createWithDummyValues(parentTable.getLocation());
    }

    public List<String> getKeys() {
      return mKeys;
    }

    public List<String> getValues() {
      return mValues;
    }

    public String getName() {
      return mPartitionName;
    }

    public String getLocation() {
      return mLocation;
    }

    public Partition toHivePartition() {
      Partition part = new Partition();
      part.setValues(mValues);
      StorageDescriptor partSd = new StorageDescriptor();
      partSd.setLocation(mLocation);
      part.setSd(partSd);
      return part;
    }
  }

  private static UdbConfiguration getUdbConfigWithGroupMountEnabled(boolean isEnabled) {
    return new UdbConfiguration(
        ImmutableMap.of(UdbProperty.GROUP_MOUNT_POINTS.getName(),
        ((Boolean) isEnabled).toString()));
  }

  private HiveDatabase createDatabaseAndMountTables(
      Map<TestTable, List<TestPartition>> tablePartitions,
      UdbConfiguration configuration)
      throws IOException {
    ImmutableMap.Builder<Table, List<Partition>> builder  = new ImmutableMap.Builder<>();
    tablePartitions.forEach((table, partitions) -> builder.put(
        table.toHiveTable(),
        partitions.stream().map(TestPartition::toHivePartition).collect(Collectors.toList())));
    mockTablesAndPartitions(mMockedClient, builder.build());
    HiveDatabase database = new HiveDatabase(
        mUdbContextWithFs,
        configuration,
        mUdbContextWithFs.getConnectionUri(),
        mUdbContextWithFs.getUdbDbName(),
        mClientPool
    );
    database.mount(
        tablePartitions.keySet().stream().map(TestTable::getName).collect(Collectors.toSet()),
        EMPTY_BYPASS_SPEC);
    return database;
  }

  private static void mockTablesAndPartitions(IMetaStoreClient mockedClient,
                                              Map<Table, List<Partition>> tablePartitions) {
    try {
      Database db = new Database();
      db.setName(DB_NAME);
      db.setLocationUri(DB_LOCATION);
      when(mockedClient.getDatabase(DB_NAME)).thenReturn(db);

      for (Map.Entry<Table, List<Partition>> entry: tablePartitions.entrySet()) {
        String tableName = entry.getKey().getTableName();
        when(mockedClient.getTable(DB_NAME, tableName)).thenReturn(entry.getKey());
        when(mockedClient.listPartitions(eq(DB_NAME), eq(tableName), eq((short) -1)))
            .thenReturn(entry.getValue());
      }
    } catch (TException e) {
      throw new IllegalStateException("Unexpected exception from a mocked client", e);
    }
  }

  private static class MockedHiveClientPool extends TestHiveClientPool {
    private final IMetaStoreClient mMockedClient;

    public MockedHiveClientPool(IMetaStoreClient mockedClient) {
      super();
      mMockedClient = mockedClient;
    }

    @Override
    protected IMetaStoreClient createNewResource() throws IOException {
      return mMockedClient;
    }
  }

  /**
   * A dummy file system that records all mount points.
   *
   * Usage notes: to test whether a table or a partition is mounted at the right location in
   * Alluxio, prepare the expected in-Alluxio path using the same logic in production code,
   * and get the actual mount point via {@link #getMountPoints()}
   * or {@link #reverseResolve(AlluxioURI)}.
   */
  private static class MountRecordingFileSystem extends DelegatingFileSystem {
    private final BiMap<AlluxioURI, AlluxioURI> mMountPoints;

    public MountRecordingFileSystem() {
      super(null);
      mMountPoints = HashBiMap.create();
    }

    public BiMap<AlluxioURI, AlluxioURI> getMountPoints() {
      return mMountPoints;
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
        throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
      // pretend the directory is created successfully and do nothing
    }

    /**
     * Given a UFS path, resolves its corresponding path in Alluxio, if the UFS path or one of
     * its ancestor directories is mounted in Alluxio.
     *
     * This should not be used in tests to get the expected in-Alluxio path of a UFS path.
     * Instead, manually create the expected path according to the logic in production code
     * that derives in-Alluxio paths from UFS paths (e.g. table locations are derived from DB name
     * and table name by {@link UdbContext#getTableLocation(String)}).
     *
     * @throws InvalidPathException when neither the UFS uri nor any of its ancestors is mounted
     */
    @Override
    public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
      if (mMountPoints.inverse().containsKey(ufsUri)) {
        return mMountPoints.inverse().get(ufsUri);
      }
      BiMap.Entry<AlluxioURI, AlluxioURI> longestPrefix = null;
      int longestPrefixDepth = -1;
      for (BiMap.Entry<AlluxioURI, AlluxioURI> entry : mMountPoints.entrySet()) {
        AlluxioURI valueUri = entry.getValue();
        if (valueUri.isAncestorOf(ufsUri) && valueUri.getDepth() > longestPrefixDepth) {
          longestPrefix = entry;
          longestPrefixDepth = valueUri.getDepth();
        }
      }
      if (longestPrefix != null) {
        String difference = PathUtils.subtractPaths(ufsUri.getPath(),
            longestPrefix.getValue().getPath());
        return longestPrefix.getKey().join(difference);
      }
      throw new InvalidPathException("Not found: " + ufsUri);
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
        throws IOException, AlluxioException {
      if (mMountPoints.containsKey(alluxioPath)) {
        throw new FileAlreadyExistsException(alluxioPath.toString());
      }
      mMountPoints.put(alluxioPath, ufsPath);
    }
  }
}
