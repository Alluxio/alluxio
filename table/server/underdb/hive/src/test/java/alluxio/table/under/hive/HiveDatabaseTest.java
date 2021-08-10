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
import static org.junit.Assert.assertTrue;
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
import alluxio.resource.CloseableResource;
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
    MountRecordingFileSystem fs = new MountRecordingFileSystem();
    mUdbContext = new UdbContext(null, fs, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);
    mUdbConf =
        new UdbConfiguration(ImmutableMap.of(UdbProperty.GROUP_MOUNT_POINTS.getName(), "false"));
    TestTable table = new TestTable("table1");
    HiveDatabase database = new HiveDatabase(
        mUdbContext,
        mUdbConf,
        mUdbContext.getConnectionUri(),
        mUdbContext.getUdbDbName(),
        new MockedHiveClientPool(
            ImmutableList.of(table.toHiveTable()),
            ImmutableList.of())
    );

    UdbBypassSpec bypassSpec = new UdbBypassSpec(ImmutableMap.of());
    database.mount(ImmutableSet.of(table.getName()), bypassSpec);
    assertEquals(mUdbContext.getTableLocation(table.getName()),
        fs.getMountPoints().inverse().get(new AlluxioURI(table.getLocation())));
  }

  @Test
  public void notGroupingMountPointsWithPartitions() throws Exception {
    MountRecordingFileSystem fs = new MountRecordingFileSystem();
    mUdbContext = new UdbContext(null, fs, DB_TYPE, CONNECTION_URI, DB_NAME, DB_NAME);
    mUdbConf =
        new UdbConfiguration(ImmutableMap.of(UdbProperty.GROUP_MOUNT_POINTS.getName(), "false"));
    TestTable table = new TestTable("table1").setPartitionKeys(ImmutableList.of("col1", "col2"));
    TestPartition partition = new TestPartition(table, ImmutableList.of("1", "2"));
    HiveDatabase database = new HiveDatabase(
        mUdbContext,
        mUdbConf,
        mUdbContext.getConnectionUri(),
        mUdbContext.getUdbDbName(),
        new MockedHiveClientPool(
            ImmutableList.of(table.toHiveTable()),
            ImmutableList.of(partition.toHivePartition()))
    );

    UdbBypassSpec bypassSpec = new UdbBypassSpec(ImmutableMap.of());
    database.mount(ImmutableSet.of(table.getName()), bypassSpec);
    AlluxioURI pathInAlluxio = fs.getMountPoints().inverse().get(new AlluxioURI(table.getLocation()));
    assertEquals(mUdbContext.getTableLocation(table.getName()), pathInAlluxio);
    assertTrue(pathInAlluxio.isAncestorOf(
        fs.reverseResolve(new AlluxioURI(table.getLocation()).join(partition.getName()))));
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

  private static class MockedHiveClientPool extends TestHiveClientPool {
    private final IMetaStoreClient mMockedClient;

    public MockedHiveClientPool(List<Table> tables, List<Partition> partitions) {
      super();
      mMockedClient = Mockito.mock(IMetaStoreClient.class);
      try {
        Database db = new Database();
        db.setName(DB_NAME);
        db.setLocationUri(CONNECTION_URI);
        when(mMockedClient.getDatabase(DB_NAME)).thenReturn(db);

        for (Table table : tables) {
          String tableName = table.getTableName();
          when(mMockedClient.getTable(DB_NAME, tableName)).thenReturn(table);
          when(mMockedClient.listPartitions(eq(DB_NAME), eq(tableName), eq((short) -1)))
              .thenReturn(partitions);
        }
      } catch (TException e) {
        // mocked client does not really throw exception
      }
    }

    @Override
    protected IMetaStoreClient createNewResource() throws IOException {
      return mMockedClient;
    }
    @Override
    public CloseableResource<IMetaStoreClient> acquireClientResource() throws IOException {
      return new CloseableResource<IMetaStoreClient>(acquire()) {
        @Override
        public void close() {
          release(get());
        }
      };
    }
  }

  /**
   * A dummy file system that records all mount points.
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
