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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.SystemPropertyRule;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.master.MasterInquireClient;
import alluxio.master.SingleMasterInquireClient.SingleMasterConnectDetails;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

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

  private FileSystemContext mMockFileSystemContext;
  private FileSystemContext mMockFileSystemContextCustomized;
  private FileSystemMasterClient mMockFileSystemMasterClient;
  private MasterInquireClient mMockMasterInquireClient;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  /**
   * Sets up the configuration before a test runs.
   */
  @Before
  public void before() throws Exception {
    mockFileSystemContextAndMasterClient();
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
    HadoopClientTestUtils.resetClient();
  }

  @Test
  public void hadoopShouldLoadFaultTolerantFileSystemWhenConfigured() throws Exception {
    URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, uri.getHost(),
        PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()),
        PropertyKey.ZOOKEEPER_ENABLED, "true",
        PropertyKey.ZOOKEEPER_ADDRESS, "ignored")).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs =
          org.apache.hadoop.fs.FileSystem.get(uri, getConf());
      assertTrue(fs instanceof FaultTolerantFileSystem);
    }
  }

  @Test
  public void hadoopShouldLoadFileSystemWithSingleZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    assertTrue(fs instanceof FileSystem);
  }

  @Test
  public void hadoopShouldLoadFileSystemWithMultipleZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@host1:2181,host2:2181,host3:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    assertTrue(fs instanceof FileSystem);

    uri = URI.create(Constants.HEADER + "zk@host1:2181;host2:2181;host3:2181/tmp/path.txt");
    fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    assertTrue(fs instanceof FileSystem);
  }

  @Test
  public void hadoopShouldLoadFileSystemWithMultiMasterUri() throws Exception {
    URI uri = URI.create("alluxio://host1:19998,host2:19998,host3:19998/path");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        alluxio.Configuration.get(PropertyKey.MASTER_RPC_ADDRESSES));
    assertTrue(fs instanceof FileSystem);

    uri = URI.create("alluxio://host1:19998;host2:19998;host3:19998/path");
    fs = org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        alluxio.Configuration.get(PropertyKey.MASTER_RPC_ADDRESSES));
    assertTrue(fs instanceof FileSystem);
  }

  @Test
  public void useSameContextWithZookeeper() throws Exception {
    URI uri = URI.create(Constants.HEADER + "dummyHost:19998/");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.ZOOKEEPER_ENABLED, "true",
        PropertyKey.ZOOKEEPER_ADDRESS, "zkAddress")).toResource()) {
      Configuration conf = getConf();
      conf.set("fs.alluxio.impl.disable.cache", "true");
      org.apache.hadoop.fs.FileSystem fs1 = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      verify(mMockFileSystemContext, times(1)).reset(alluxio.Configuration.global());
      // The filesystem context should return a master inquire client based on the latest config
      when(mMockFileSystemContext.getMasterInquireClient())
          .thenReturn(MasterInquireClient.Factory.create());
      // The first initialize should reset the context, but later initializes should not.
      org.apache.hadoop.fs.FileSystem.get(uri, conf);
      verify(mMockFileSystemContext, times(1)).reset(alluxio.Configuration.global());
    }
  }

  /**
   * Hadoop should be able to load uris like alluxio-ft:///path/to/file.
   */
  @Test
  public void loadFaultTolerantSystemWhenUsingNoAuthority() throws Exception {
    URI uri = URI.create(Constants.HEADER_FT + "/tmp/path.txt");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.ZOOKEEPER_ENABLED, "true",
        PropertyKey.ZOOKEEPER_ADDRESS, "ignored")).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs =
          org.apache.hadoop.fs.FileSystem.get(uri, getConf());
      assertTrue(fs instanceof FaultTolerantFileSystem);
    }
  }

  /**
   * Tests that using an alluxio-ft:/// URI is still possible after using an alluxio://host:port/
   * URI.
   */
  @Test
  public void loadRegularThenFaultTolerant() throws Exception {
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.ZOOKEEPER_ENABLED, "true",
        PropertyKey.ZOOKEEPER_ADDRESS, "host:2")).toResource()) {
      org.apache.hadoop.fs.FileSystem.get(URI.create(Constants.HEADER + "host:1/"), getConf());
      org.apache.hadoop.fs.FileSystem fs =
          org.apache.hadoop.fs.FileSystem.get(URI.create(Constants.HEADER_FT + "/"), getConf());
      assertTrue(fs instanceof FaultTolerantFileSystem);
    }
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
    try (Closeable c = new ConfigurationRule(properties).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      assertTrue(fs instanceof FileSystem);
    }
  }

  /**
   * Tests that initializing the {@link AbstractFileSystem} will reinitialize the file system
   * context.
   */
  @Test
  public void resetContext() throws Exception {
    // Change to otherhost:410
    URI uri = URI.create(Constants.HEADER + "otherhost:410/");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    verify(mMockFileSystemContext).reset(alluxio.Configuration.global());
  }

  @Test
  public void resetContextUsingZookeeperUris() throws Exception {
    // Change to signle zookeeper uri
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "zk@host1:2181,host2:2181,host3:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "zk@host1:2181;host2:2181;host3:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());
    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:2181,host2:2181,host3:2181",
        alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));

    verify(mMockFileSystemContext, times(3)).reset(alluxio.Configuration.global());
  }

  @Test
  public void resetContextFromZkUriToNonZkUri() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));

    URI otherUri = URI.create(Constants.HEADER + "alluxioHost:19998/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem otherFs = org.apache.hadoop.fs.FileSystem.get(otherUri, conf);
    assertEquals("alluxioHost", alluxio.Configuration.get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("19998", alluxio.Configuration.get(PropertyKey.MASTER_RPC_PORT));
    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertFalse(alluxio.Configuration.isSet(PropertyKey.ZOOKEEPER_ADDRESS));
    verify(mMockFileSystemContext, times(2)).reset(alluxio.Configuration.global());
  }

  @Test
  public void resetContextUsingMultiMasterUris() throws Exception {
    // Change to multi-master uri
    URI uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("host1:19998,host2:19998,host3:19998",
        alluxio.Configuration.get(PropertyKey.MASTER_RPC_ADDRESSES));

    verify(mMockFileSystemContext).reset(alluxio.Configuration.global());
  }

  @Test
  public void resetContextFromZookeeperToMultiMaster() throws Exception {
    URI uri = URI.create(Constants.HEADER + "zk@zkHost:2181/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, getConf());
    assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));

    uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(3,
        ConfigurationUtils.getMasterRpcAddresses(alluxio.Configuration.global()).size());
    assertEquals("host1:19998,host2:19998,host3:19998",
        alluxio.Configuration.get(PropertyKey.MASTER_RPC_ADDRESSES));

    verify(mMockFileSystemContext, times(2)).reset(alluxio.Configuration.global());
  }

  @Test
  public void resetContextFromMultiMasterToSingleMaster() throws Exception {
    URI uri = URI.create(Constants.HEADER + "host1:19998,host2:19998,host3:19998/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(3,
        ConfigurationUtils.getMasterRpcAddresses(alluxio.Configuration.global()).size());
    assertEquals("host1:19998,host2:19998,host3:19998",
        alluxio.Configuration.get(PropertyKey.MASTER_RPC_ADDRESSES));

    uri = URI.create(Constants.HEADER + "host:19998/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    assertFalse(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals(PropertyKey.MASTER_JOURNAL_TYPE.getDefaultValue(),
        alluxio.Configuration.get(PropertyKey.MASTER_JOURNAL_TYPE));
    assertEquals(1,
        ConfigurationUtils.getMasterRpcAddresses(alluxio.Configuration.global()).size());

    verify(mMockFileSystemContext, times(2)).reset(alluxio.Configuration.global());
  }

  /**
   * Verifies that the initialize method is only called once even when there are many concurrent
   * initializers during the initialization phase.
   */
  @Test
  public void concurrentInitialize() throws Exception {
    List<Thread> threads = new ArrayList<>();
    final org.apache.hadoop.conf.Configuration conf = getConf();
    when(mMockFileSystemContext.getMasterAddress())
        .thenReturn(new InetSocketAddress("randomhost", 410));
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
    org.apache.hadoop.fs.FileSystem.get(uri, conf);
    assertEquals("host1", alluxio.Configuration.get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("1", alluxio.Configuration.get(PropertyKey.MASTER_RPC_PORT));

    uri = URI.create("alluxio://host2:2");
    org.apache.hadoop.fs.FileSystem.get(uri, conf);
    assertEquals("host2", alluxio.Configuration.get(PropertyKey.MASTER_HOSTNAME));
    assertEquals("2", alluxio.Configuration.get(PropertyKey.MASTER_RPC_PORT));
  }

  /**
   * Tests that the {@link AbstractFileSystem#listStatus(Path)} method uses
   * {@link URIStatus#getLastModificationTimeMs()} correctly.
   */
  @Test
  public void listStatus() throws Exception {
    FileInfo fileInfo1 = new FileInfo()
        .setLastModificationTimeMs(111L)
        .setFolder(false)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(00755);
    FileInfo fileInfo2 = new FileInfo()
        .setLastModificationTimeMs(222L)
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
    // FileSystem.get would have thrown an exception if the initialization failed.
  }

  @Test
  public void initializeWithFullPrincipalUgi() throws Exception {
    mockUserGroupInformation("testuser@ALLUXIO.COM");

    final org.apache.hadoop.conf.Configuration conf = getConf();
    URI uri = URI.create(Constants.HEADER + "host:1");
    org.apache.hadoop.fs.FileSystem.get(uri, conf);
    // FileSystem.get would have thrown an exception if the initialization failed.
  }

  @Test
  public void initializeWithZookeeperSystemProperties() throws Exception {
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), "true");
    sysProps.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), "zkHost:2181");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      alluxio.Configuration.reset();
      URI uri = URI.create("alluxio:///");
      org.apache.hadoop.fs.FileSystem.get(uri, getConf());
      assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    }
  }

  @Test
  public void initializeWithZookeeperUriAndSystemProperty() throws Exception {
    // When URI and system property both have Zookeeper configuration,
    // those in the URI has the highest priority.
    try (Closeable p = new SystemPropertyRule(
         PropertyKey.ZOOKEEPER_ENABLED.getName(), "false").toResource()) {
      alluxio.Configuration.reset();
      URI uri = URI.create("alluxio://zk@zkHost:2181");
      org.apache.hadoop.fs.FileSystem.get(uri, getConf());
      assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    }

    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), "true");
    sysProps.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), "zkHost1:2181");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      alluxio.Configuration.reset();
      URI uri = URI.create("alluxio://zk@zkHost2:2181");
      org.apache.hadoop.fs.FileSystem.get(uri, getConf());
      assertTrue(alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      assertEquals("zkHost2:2181", alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
    }
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

    List<WorkerNetAddress> expectedWorkers = Collections.EMPTY_LIST;

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
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
        new ConfigurationRule(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED, "true")
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

    List<WorkerNetAddress> expectedWorkers = Collections.EMPTY_LIST;

    verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
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
        new ConfigurationRule(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED, "true")
            .toResource()) {
      verifyBlockLocations(blockWorkers, ufsLocations, allWorkers, expectedWorkers);
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
        .setFolder(false)
        .setOwner("user1")
        .setGroup("group1")
        .setMode(00755)
        .setFileBlockInfos(Arrays.asList(blockInfo));
    Path path = new Path("/dir/file");
    alluxio.client.file.FileSystem alluxioFs =
        mock(alluxio.client.file.FileSystem.class);
    when(alluxioFs.getStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(new URIStatus(fileInfo));
    AlluxioBlockStore blockStore = mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(null)).thenReturn(blockStore);
    List<BlockWorkerInfo> eligibleWorkerInfos = allWorkers.stream().map(worker ->
        new BlockWorkerInfo(worker, 0, 0)).collect(toList());
    PowerMockito.when(blockStore.getEligibleWorkers()).thenReturn(eligibleWorkerInfos);
    List<HostAndPort> expectedWorkerNames = expectedWorkers.stream()
        .map(addr -> HostAndPort.fromParts(addr.getHost(), addr.getDataPort())).collect(toList());
    FileSystem alluxioHadoopFs = new FileSystem(alluxioFs);
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
      conf.set("fs." + Constants.SCHEME_FT + ".impl", FaultTolerantFileSystem.class.getName());
    }
    return conf;
  }

  private void mockFileSystemContextAndMasterClient() throws Exception {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockFileSystemContextCustomized = PowerMockito.mock(FileSystemContext.class);
    mMockMasterInquireClient = Mockito.mock(MasterInquireClient.class);
    when(mMockMasterInquireClient.getConnectDetails()).thenReturn(
        new SingleMasterConnectDetails(new InetSocketAddress("defaultHost", 1)));
    PowerMockito.mockStatic(FileSystemContext.class);
    PowerMockito.when(FileSystemContext.get()).thenReturn(mMockFileSystemContext);
    PowerMockito.when(FileSystemContext.get(any(Subject.class)))
        .thenReturn(mMockFileSystemContextCustomized);
    PowerMockito.when(FileSystemContext.get()).thenReturn(mMockFileSystemContext);
    mMockFileSystemMasterClient = mock(FileSystemMasterClient.class);
    when(mMockFileSystemContext.acquireMasterClient())
        .thenReturn(mMockFileSystemMasterClient);
    when(mMockFileSystemContextCustomized.acquireMasterClient())
        .thenReturn(mMockFileSystemMasterClient);
    when(mMockFileSystemContext.getMasterInquireClient()).thenReturn(mMockMasterInquireClient);
    doNothing().when(mMockFileSystemMasterClient).connect();
    when(mMockFileSystemContext.getMasterAddress())
        .thenReturn(new InetSocketAddress("defaultHost", 1));
  }

  private void mockUserGroupInformation(String username) throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
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
    assertEquals(info.isFolder(), status.isDir());
  }
}
