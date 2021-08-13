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
import com.google.common.collect.ImmutableSet;
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
import java.util.stream.Collectors;

public class HiveDatabaseTest {

  private static final String DB_NAME = "test";
  public static final String DB_TYPE = "hive";
  public static final String CONNECTION_URI = "thrift://not_running:9083";
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
  public void notGroupingMountPointsTablesOnly() throws Exception {
    TestTable table = new TestTable("table1");
    mockTablesAndPartitions(mMockedClient,
        ImmutableList.of(table.toHiveTable()), ImmutableList.of());
    UdbContext udbContext = new UdbContext(null, mFs, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);
    UdbConfiguration udbConf =
        new UdbConfiguration(ImmutableMap.of(UdbProperty.GROUP_MOUNT_POINTS.getName(), "false"));
    HiveDatabase database = new HiveDatabase(
        udbContext,
        udbConf,
        udbContext.getConnectionUri(),
        udbContext.getUdbDbName(),
        mClientPool
    );
    database.mount(ImmutableSet.of(table.getName()), EMPTY_BYPASS_SPEC);

    AlluxioURI expectedTableLocation = udbContext.getTableLocation(table.getName());
    AlluxioURI actualTableLocation =
        mFs.getMountPoints().inverse().get(new AlluxioURI(table.getLocation()));
    assertEquals(expectedTableLocation, actualTableLocation);
  }

  @Test
  public void notGroupingMountPointsWithPartitions() throws Exception {
    TestTable table = new TestTable("table1").setPartitionKeys(ImmutableList.of("col1", "col2"));
    TestPartition partition = new TestPartition(table, ImmutableList.of("1", "2"));
    mockTablesAndPartitions(mMockedClient,
        ImmutableList.of(table.toHiveTable()),
        ImmutableList.of(partition.toHivePartition()));
    UdbContext udbContext = new UdbContext(null, mFs, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);
    UdbConfiguration udbConf =
        new UdbConfiguration(ImmutableMap.of(UdbProperty.GROUP_MOUNT_POINTS.getName(), "false"));
    HiveDatabase database = new HiveDatabase(
        udbContext,
        udbConf,
        udbContext.getConnectionUri(),
        udbContext.getUdbDbName(),
        mClientPool
    );
    database.mount(ImmutableSet.of(table.getName()), EMPTY_BYPASS_SPEC);

    AlluxioURI actualTableMountPoint =
        mFs.getMountPoints().inverse().get(new AlluxioURI(table.getLocation()));
    AlluxioURI expectedTableMountPoint = udbContext.getTableLocation(table.getName());
    assertEquals(expectedTableMountPoint, actualTableMountPoint);

    AlluxioURI expectedPartitionMountPoint = expectedTableMountPoint.join(partition.getName());
    AlluxioURI actualPartitionMountPoint =
        mFs.reverseResolve(new AlluxioURI(partition.getLocation()));
    assertEquals(actualPartitionMountPoint, expectedPartitionMountPoint);
  }

  private static class TestTable {
    private final String mName;
    private String mLocation;
    private List<String> mPartitionKeys;

    public TestTable(String name) {
      mName = name;
      mLocation = PathUtils.concatPath(CONNECTION_URI, DB_NAME, mName);
      mPartitionKeys = ImmutableList.of();
    }

    public TestTable(TestTable table) {
      mName = table.mName;
      mLocation = table.mLocation;
      mPartitionKeys = table.mPartitionKeys;
    }

    public String getName() {
      return mName;
    }

    public String getLocation() {
      return mLocation;
    }

    public TestTable setLocationPrefix(String locationPrefix) {
      mLocation = PathUtils.concatPath(locationPrefix, mName);
      return this;
    }

    public List<String> getPartitionKeys() {
      return mPartitionKeys;
    }

    public TestTable setPartitionKeys(List<String> partitionKeys) {
      mPartitionKeys = partitionKeys;
      return this;
    }

    public Table toHiveTable() {
      Table table = new Table();
      table.setTableName(mName);
      table.setDbName(DB_NAME);
      table.setPartitionKeys(mPartitionKeys.stream().map((key) -> {
        FieldSchema schema = new FieldSchema();
        schema.setName(key);
        return schema;
      }).collect(Collectors.toList()));
      StorageDescriptor tableSd = new StorageDescriptor();
      tableSd.setLocation(mLocation);
      table.setSd(tableSd);
      return table;
    }
  }

  private static class TestPartition {
    private final List<String> mValues;
    private final String mPartitionName;
    private String mLocation;

    public TestPartition(TestTable parentTable, List<String> values) {
      Preconditions.checkArgument(parentTable.getPartitionKeys().size() == values.size());
      mValues = values;
      mPartitionName = FileUtils.makePartName(parentTable.getPartitionKeys(), mValues);
      mLocation = PathUtils.concatPath(parentTable.getLocation(), mPartitionName);
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

    public TestPartition setLocationPrefix(String locationPrefix) {
      mLocation = PathUtils.concatPath(locationPrefix, mPartitionName);
      return this;
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

  private static void mockTablesAndPartitions(IMetaStoreClient mockedClient,
                                              List<Table> tables,
                                              List<Partition> partitions) {
    try {
      Database db = new Database();
      db.setName(DB_NAME);
      db.setLocationUri(CONNECTION_URI);
      when(mockedClient.getDatabase(DB_NAME)).thenReturn(db);

      for (Table table : tables) {
        String tableName = table.getTableName();
        when(mockedClient.getTable(DB_NAME, tableName)).thenReturn(table);
        when(mockedClient.listPartitions(eq(DB_NAME), eq(tableName), eq((short) -1)))
            .thenReturn(partitions);
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
