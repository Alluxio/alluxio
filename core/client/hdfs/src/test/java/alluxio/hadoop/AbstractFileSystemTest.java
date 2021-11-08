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

package alluxio.hadoop;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.SystemPropertyRule;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link AbstractFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, FileSystemMasterClient.class,
    UserGroupInformation.class})
/*
 * [ALLUXIO-1384] Tell PowerMock to defer the loading of javax.security classes to the system
 * classloader in order to avoid linkage error when running this test with CDH.
 * See https://code.google.com/p/powermock/wiki/FAQ.
 */
@PowerMockIgnore("javax.security.*")
/**
 * Tests for {@link AbstractFileSystem}.
 */
public class AbstractFileSystemTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileSystemTest.class);

  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  /**
   * Sets up the configuration before a test runs.
   */
  @Before
  public void before() throws Exception {
    mockUserGroupInformation("");

    if (HadoopClientTestUtils.isHadoop1x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 1x");
    } else if (HadoopClientTestUtils.isHadoop2x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 2x");
    } else {
      LOG.warn("Running Alluxio FS tests against untargeted Hadoop version: "
          + HadoopClientTestUtils.getHadoopVersion());
    }
  }

  @After
  public void after() {
    mConfiguration = ConfigurationTestUtils.defaults();
    HadoopClientTestUtils.disableMetrics(mConfiguration);
  }

  @Test
  public void hadoopShouldLoadFileSystemWithSingleZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");

    FileSystem hfs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, conf));
    assertTrue(hfs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", hfs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  @Test
  public void hadoopShouldLoadFileSystemWithMultipleZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@host1:2181,host2:2181,host3:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    FileSystem hfs = getHadoopFilesystem(fs);
    assertTrue(hfs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        hfs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "zk@host1:2181;host2:2181;host3:2181/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, conf));
    assertTrue(hfs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        hfs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  @Test
  public void fsShouldSetPropertyConfWithLogicalUriConfig() throws Exception {
    URI uri = URI.create("alluxio://ebj@logical/path");
    Configuration conf = getConf();
    conf.set(PropertyKey.Template.MASTER_LOGICAL_NAMESERVICES.format("logical").getName(),
        "master1,master2,master3");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_RPC_ADDRESS.format("logical", "master1").getName(),
        "host1:19998");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_RPC_ADDRESS.format("logical", "master2").getName(),
        "host2:19998");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_RPC_ADDRESS.format("logical", "master3").getName(),
        "host3:19998");
    AbstractFileSystem afs = new alluxio.hadoop.FileSystem();
    afs.initialize(uri, conf);
    assertEquals("host1:19998,host2:19998,host3:19998",
        afs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));
    assertFalse(afs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
  }

  @Test
  public void fsShouldTriggersExceptionWithUnknownLogicalUriWith() throws Exception {
    URI uri = URI.create("alluxio://ebj@logical/path");
    AbstractFileSystem afs = new alluxio.hadoop.FileSystem();
    assertThrows(Exception.class, () -> afs.initialize(uri, getConf()));
  }

  @Test
  public void fsShouldSetPropertyConfWithZkLogicalUriConfig() throws Exception {
    URI uri = URI.create("alluxio://zk@logical/path");
    Configuration conf = getConf();
    conf.set(PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_NAMESERVICES.format("logical").getName(),
        "node1,node2,node3");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_ADDRESS.format("logical", "node1").getName(),
        "host1:2181");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_ADDRESS.format("logical", "node2").getName(),
        "host2:2181");
    conf.set(
        PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_ADDRESS.format("logical", "node3").getName(),
        "host3:2181");
    AbstractFileSystem afs = new alluxio.hadoop.FileSystem();
    afs.initialize(uri, conf);
    assertEquals("host1:2181,host2:2181,host3:2181",
        afs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
    assertTrue(afs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
  }

  @Test
  public void fsShouldTriggersExceptionWithUnknownZkLogicalUriWith() {
    URI uri = URI.create("alluxio://zk@logical/path");
    AbstractFileSystem afs = new alluxio.hadoop.FileSystem();
    assertThrows(Exception.class, () -> afs.initialize(uri, getConf()));
  }

  @Test
  public void fsShouldSetPropertyConfWithMultiMasterUri() throws Exception {
    URI uri = URI.create("alluxio://host1:19998,host2:19998,host3:19998/path");
    AbstractFileSystem afs = new alluxio.hadoop.FileSystem();
    afs.initialize(uri, getConf());
    assertFalse(afs.mFileSystem.getConf()
        .getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        afs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));

    uri = URI.create("alluxio://host1:19998;host2:19998;host3:19998/path");
    afs = new FileSystem();
    afs.initialize(uri, getConf());

    assertFalse(afs.mFileSystem.getConf()
        .getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        afs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));
  }

  @Test
  public void hadoopShouldLoadFsWithMultiMasterUri() throws Exception {
    URI uri = URI.create("alluxio://host1:19998,host2:19998,host3:19998/path");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, getConf());
    assertTrue(fs instanceof FileSystem);

    uri = URI.create("alluxio://host1:19998;host2:19998;host3:19998/path");
    fs = org.apache.hadoop.fs.FileSystem.get(uri, getConf());
    assertTrue(fs instanceof FileSystem);
  }

  @Test
  public void hadoopShouldLoadFileSystemWhenConfigured() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();

    URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    Map<PropertyKey, String> properties = new HashMap<>();
    properties.put(PropertyKey.MASTER_HOSTNAME, uri.getHost());
    properties.put(PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    properties.put(PropertyKey.ZOOKEEPER_ENABLED, "false");
    properties.put(PropertyKey.ZOOKEEPER_ADDRESS, null);
    try (Closeable c = new ConfigurationRule(properties, mConfiguration).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      assertTrue(fs instanceof FileSystem);
    }
  }

  @Test
  public void resetContextUsingZookeeperUris() throws Exception {
    // Change to signle zookeeper uri
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

    assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "zk@host1:2181,host2:2181,host3:2181/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

    assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "zk@host1:2181;host2:2181;host3:2181/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));
    assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  @Test
  public void resetContextFromZkUriToNonZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, conf));
    assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));

    URI otherUri = URI.create(Constants.HEADER + "alluxioHost:19998/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(otherUri, conf));
    assertEquals("alluxioHost", fs.mFileSystem.getConf().get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("19998", fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_PORT));
    assertFalse(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertFalse(fs.mFileSystem.getConf().isSet(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  @Test
  public void resetContextUsingMultiMasterUris() throws Exception {
    // Change to multi-master uri
    URI uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));
    assertFalse(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));
  }

  @Test
  public void resetContextFromZookeeperToMultiMaster() throws Exception {
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));
    assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

    assertFalse(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(3,
        ConfigurationUtils.getMasterRpcAddresses(fs.mFileSystem.getConf()).size());
    assertEquals("host1:19998,host2:19998,host3:19998",
        fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));
  }

  @Test
  public void resetContextFromMultiMasterToSingleMaster() throws Exception {
    URI uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

    assertFalse(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(3,
        ConfigurationUtils.getMasterRpcAddresses(fs.mFileSystem.getConf()).size());
    assertEquals("host1:19998,host2:19998,host3:19998",
        fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_ADDRESSES));

    uri = URI.create(Constants.HEADER + "host:19998/tmp/path.txt");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

    assertFalse(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(PropertyKey.MASTER_JOURNAL_TYPE.getDefaultValue(),
        fs.mFileSystem.getConf().get(PropertyKey.MASTER_JOURNAL_TYPE));
    assertEquals(1,
        ConfigurationUtils.getMasterRpcAddresses(fs.mFileSystem.getConf()).size());
  }

  /**
   * Verifies that the initialize method is only called once even when there are many concurrent
   * initializers during the initialization phase.
   */
  @Test
  public void concurrentInitialize() throws Exception {
    List<Thread> threads = new ArrayList<>();
    final org.apache.hadoop.conf.Configuration conf = getConf();
    for (int i = 0; i < 100; i++) {
      Thread t = new Thread(() -> {
        URI uri = URI.create(Constants.HEADER + "randomhost:410/");
        try {
          org.apache.hadoop.fs.FileSystem.get(uri, conf);
        } catch (IOException e) {
          fail();
        }
      });
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }

  /**
   * Tests that after initialization, reinitialize with a different URI.
   */
  @Test
  public void reinitializeWithDifferentURI() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create("alluxio://host1:1");
    FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, conf));
    assertEquals("host1", fs.mFileSystem.getConf().get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("1", fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_PORT));

    uri = URI.create("alluxio://host2:2");
    fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, conf));
    assertEquals("host2", fs.mFileSystem.getConf().get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("2", fs.mFileSystem.getConf().get(PropertyKey.MASTER_RPC_PORT));
  }

  /**
   * Tests that the {@link AbstractFileSystem#listStatus(Path)} method uses
   * {@link URIStatus#getLastModificationTimeMs()} correctly.
   */
  @Test
  public void listStatus() throws Exception {
    FileInfo fileInfo1 = new FileInfo()
        .setLastModificationTimeMs(111L)
        .setLastAccessTimeMs(123L)
        .setFolder(false)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(00755);
    FileInfo fileInfo2 = new FileInfo()
        .setLastModificationTimeMs(222L)
        .setLastAccessTimeMs(234L)
        .setFolder(true)
        .setOwner("user2")
        .setGroup("group2")
        .setMode(00644);

    Path path = new Path("/dir");
    alluxio.client.file.FileSystem alluxioFs =
        mock(alluxio.client.file.FileSystem.class);
    when(alluxioFs.listStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(Lists.newArrayList(new URIStatus(fileInfo1), new URIStatus(fileInfo2)));
    FileSystem alluxioHadoopFs = new FileSystem(alluxioFs);

    FileStatus[] fileStatuses = alluxioHadoopFs.listStatus(path);
    assertFileInfoEqualsFileStatus(fileInfo1, fileStatuses[0]);
    assertFileInfoEqualsFileStatus(fileInfo2, fileStatuses[1]);
    alluxioHadoopFs.close();
  }

  /**
   * Tests that the {@link AbstractFileSystem#listStatus(Path)} method throws
   * FileNotFound Exception.
   */
  @Test
  public void throwFileNotFoundExceptionWhenListStatusNonExistingTest() throws Exception {
    FileSystem alluxioHadoopFs = null;
    try {
      Path path = new Path("/ALLUXIO-2036");
      alluxio.client.file.FileSystem alluxioFs = mock(alluxio.client.file.FileSystem.class);
      when(alluxioFs.listStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenThrow(new FileNotFoundException("ALLUXIO-2036 not Found"));
      alluxioHadoopFs = new FileSystem(alluxioFs);
      FileStatus[] fileStatuses = alluxioHadoopFs.listStatus(path);
      // if we reach here, FileNotFoundException is not thrown hence Fail the test case
      assertTrue(false);
    } catch (FileNotFoundException fnf) {
      assertEquals("ALLUXIO-2036 not Found", fnf.getMessage());
    } finally {
      if (null != alluxioHadoopFs) {
        try {
          alluxioHadoopFs.close();
        } catch (Exception ex) {
          // nothing to catch, ignore it.
        }
      }
    }
  }

  @Test
  public void getStatus() throws Exception {
    FileInfo fileInfo = new FileInfo()
        .setLastModificationTimeMs(111L)
        .setLastAccessTimeMs(123L)
        .setFolder(false)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(00755);

    Path path = new Path("/dir");
    alluxio.client.file.FileSystem alluxioFs =
        mock(alluxio.client.file.FileSystem.class);
    when(alluxioFs.getStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(new URIStatus(fileInfo));
    FileSystem alluxioHadoopFs = new FileSystem(alluxioFs);

    FileStatus fileStatus = alluxioHadoopFs.getFileStatus(path);
    assertFileInfoEqualsFileStatus(fileInfo, fileStatus);
  }

  @Test
  public void initializeWithCustomizedUgi() throws Exception {
    mockUserGroupInformation("testuser");

    final org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "host:1");
    org.apache.hadoop.fs.FileSystem.get(uri, conf);
    // FileSystem.create would have thrown an exception if the initialization failed.
  }

  @Test
  public void initializeWithFullPrincipalUgi() throws Exception {
    mockUserGroupInformation("testuser@ALLUXIO.COM");

    final org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "host:1");
    org.apache.hadoop.fs.FileSystem.get(uri, conf);
    // FileSystem.create would have thrown an exception if the initialization failed.
  }

  @Test
  public void initializeWithZookeeperSystemProperties() throws Exception {
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), "true");
    sysProps.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), "zkHost:2181");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      ConfigurationUtils.reloadProperties();
      URI uri = URI.create("alluxio:///");
      FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));

      assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
    }
  }

  @Test
  public void initializeWithZookeeperUriAndSystemProperty() throws Exception {
    // When URI and system property both have Zookeeper configuration,
    // those in the URI has the highest priority.
    try (Closeable p = new SystemPropertyRule(
         PropertyKey.ZOOKEEPER_ENABLED.getName(), "false").toResource()) {
      ConfigurationUtils.reloadProperties();
      URI uri = URI.create("alluxio://zk@zkHost:2181");
      FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));
      assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
      fs.close();
    }

    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), "true");
    sysProps.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), "zkHost1:2181");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      ConfigurationUtils.reloadProperties();
      URI uri = URI.create("alluxio://zk@zkHost2:2181");
      FileSystem fs = getHadoopFilesystem(org.apache.hadoop.fs.FileSystem.get(uri, getConf()));
      assertTrue(fs.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost2:2181", fs.mFileSystem.getConf().get(PropertyKey.ZOOKEEPER_ADDRESS));
      fs.close();
    }
    ConfigurationUtils.reloadProperties();
  }

  @Test
  public void getBlockLocationsOnlyInAlluxio() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList(worker1);
    List<String> ufsLocations = Arrays.asList();
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker1);

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
  }

  @Test
  public void getBlockLocationsInUfs() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList(worker2.getHost());
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker2);

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
  }

  @Test
  public void getBlockLocationsInUfsAndAlluxio() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList(worker1);
    List<String> ufsLocations = Arrays.asList(worker2.getHost());
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker1);

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
  }

  @Test
  public void getBlockLocationsOnlyMatchingWorkers() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList("worker0", worker2.getHost(), "worker3");
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker2);

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
  }

  @Test
  public void getBlockLocationsNoMatchingWorkersDefault() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList("worker0", "worker3");
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    // When no matching, all workers will be returned
    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, allWorkers);
  }

  @Test
  public void getBlockLocationsNoMatchingWorkersWithFallback() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList("worker0", "worker3");
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker1, worker2);

    try (Closeable conf =
        new ConfigurationRule(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED, "true",
            mConfiguration)
            .toResource()) {
      verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
    }
  }

  @Test
  public void getBlockLocationsNoUfsLocationsDefault() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList();
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    // When no matching & no ufs locations, all workers will be returned
    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, allWorkers);
  }

  @Test
  public void getBlockLocationsNoUfsLocationsWithFallback() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1").setDataPort(1234);
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2").setDataPort(1234);
    List<WorkerNetAddress> blockWorkers = Arrays.asList();
    List<String> ufsLocations = Arrays.asList();
    List<WorkerNetAddress> allWorkers = Arrays.asList(worker1, worker2);

    List<WorkerNetAddress> expectedWorkers = Arrays.asList(worker1, worker2);

    try (Closeable conf =
        new ConfigurationRule(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED, "true",
            mConfiguration)
            .toResource()) {
      verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
    }
  }

  @Test
  public void appendExistingNotSupported() throws Exception {
    Path path = new Path("/file");
    alluxio.client.file.FileSystem alluxioFs =
        mock(alluxio.client.file.FileSystem.class);
    when(alluxioFs.exists(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(true);

    try (FileSystem alluxioHadoopFs = new FileSystem(alluxioFs)) {
      alluxioHadoopFs.append(path, 100);
      fail("append() of existing file is expected to fail");
    } catch (IOException e) {
      assertEquals("append() to existing Alluxio path is currently not supported: " + path,
          e.getMessage());
    }
  }

  @Test
  public void createWithoutOverwrite() throws Exception {
    Path path = new Path("/file");
    alluxio.client.file.FileSystem alluxioFs =
        mock(alluxio.client.file.FileSystem.class);
    when(alluxioFs.exists(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(true);
    when(alluxioFs.createFile(eq(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))), any()))
        .thenThrow(new FileAlreadyExistsException(path.toString()));

    try (FileSystem alluxioHadoopFs = new FileSystem(alluxioFs)) {
      alluxioHadoopFs.create(path, false, 100, (short) 1, 1000);
      fail("create() of existing file is expected to fail");
    } catch (IOException e) {
      assertEquals("Not allowed to create() (overwrite=false) for existing Alluxio path: " + path,
          e.getMessage());
    }
  }

  void verifyBlockLocations(List<WorkerNetAddress> blockWorkers, List<String> ufsLocations,
      List<WorkerNetAddress> allWorkers, List<WorkerNetAddress> expectedWorkers) throws Exception {
    FileBlockInfo blockInfo = new FileBlockInfo().setBlockInfo(
        new BlockInfo().setLocations(blockWorkers.stream().map(
            addr -> new alluxio.wire.BlockLocation().setWorkerAddress(addr)).collect(
            toList()))).setUfsLocations(ufsLocations);
    FileInfo fileInfo = new FileInfo()
        .setLastModificationTimeMs(111L)
        .setLastAccessTimeMs(123L)
        .setFolder(false)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(00755)
        .setFileBlockInfos(Arrays.asList(blockInfo));
    Path path = new Path("/dir/file");
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    AlluxioBlockStore blockStore = mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(any(FileSystemContext.class)))
        .thenReturn(blockStore);
    FileSystemContext fsContext = mock(FileSystemContext.class);
    when(fsContext.getClientContext()).thenReturn(ClientContext.create(mConfiguration));
    when(fsContext.getClusterConf()).thenReturn(mConfiguration);
    when(fsContext.getPathConf(any(AlluxioURI.class))).thenReturn(mConfiguration);
    alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.create(fsContext);
    alluxio.client.file.FileSystem spyFs = spy(fs);
    doReturn(new URIStatus(fileInfo)).when(spyFs).getStatus(uri);
    List<BlockWorkerInfo> eligibleWorkerInfos = allWorkers.stream().map(worker ->
        new BlockWorkerInfo(worker, 0, 0)).collect(toList());
    when(fsContext.getCachedWorkers()).thenReturn(eligibleWorkerInfos);
    List<HostAndPort> expectedWorkerNames = expectedWorkers.stream()
        .map(addr -> HostAndPort.fromParts(addr.getHost(), addr.getDataPort())).collect(toList());
    FileSystem alluxioHadoopFs = new FileSystem(spyFs);
    FileStatus file = new FileStatus(0, false, 0, 0, 0, 0, null, null, null, path);
    long start = 0;
    long len = 100;
    BlockLocation[] locations = alluxioHadoopFs.getFileBlockLocations(file, start, len);
    assertEquals(1, locations.length);
    Collections.sort(expectedWorkerNames, (x, y) -> x.toString().compareTo(y.toString()));
    String[] actualNames = locations[0].getNames();
    String[] actualHosts = locations[0].getHosts();
    Arrays.sort(actualNames);
    Arrays.sort(actualHosts);
    assertArrayEquals(expectedWorkerNames.stream().map(HostAndPort::toString).toArray(),
        actualNames);
    assertArrayEquals(expectedWorkerNames.stream().map(HostAndPort::getHost).toArray(),
        actualHosts);
    alluxioHadoopFs.close();
  }

  private org.apache.hadoop.conf.Configuration getConf() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (HadoopClientTestUtils.isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", FileSystem.class.getName());
    }
    return conf;
  }

  private FileSystem getHadoopFilesystem(org.apache.hadoop.fs.FileSystem fs) {
    assertTrue(fs instanceof FileSystem);
    return (FileSystem) fs;
  }

  private void mockUserGroupInformation(String username) throws IOException {
    // need to mock out since FileSystem.create calls UGI, which occasionally has issues on some
    // systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = mock(UserGroupInformation.class);
    when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    when(ugi.getUserName()).thenReturn(username);
    when(ugi.getShortUserName()).thenReturn(username.split("@")[0]);
  }

  private void assertFileInfoEqualsFileStatus(FileInfo info, FileStatus status) {
    assertEquals(info.getOwner(), status.getOwner());
    assertEquals(info.getGroup(), status.getGroup());
    assertEquals(info.getMode(), status.getPermission().toShort());
    assertEquals(info.getLastModificationTimeMs(), status.getModificationTime());
    assertEquals(info.getLastAccessTimeMs(), status.getAccessTime());
    assertEquals(info.isFolder(), status.isDir());
  }
}
