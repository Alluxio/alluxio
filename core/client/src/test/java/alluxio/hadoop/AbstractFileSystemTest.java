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

import alluxio.AlluxioURI;
import alluxio.CommonTestUtils;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.lineage.LineageContext;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.ConnectionFailedException;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Assert;
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
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

/**
 * Unit tests for {@link AbstractFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, UserGroupInformation.class})
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
    ClientTestUtils.resetClient();
  }

  /**
   * Ensures that Hadoop loads {@link FaultTolerantFileSystem} when configured.
   */
  @Test
  public void hadoopShouldLoadFaultTolerantFileSystemWhenConfigured() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (HadoopClientTestUtils.isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", FaultTolerantFileSystem.class.getName());
    }

    URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, uri.getHost(),
        PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()),
        PropertyKey.ZOOKEEPER_ENABLED, "true")).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      Assert.assertTrue(fs instanceof FaultTolerantFileSystem);
    }
  }

  /**
   * Hadoop should be able to load uris like alluxio-ft:///path/to/file.
   */
  @Test
  public void loadFaultTolerantSystemWhenUsingNoAuthority() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (HadoopClientTestUtils.isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", FaultTolerantFileSystem.class.getName());
    }

    URI uri = URI.create(Constants.HEADER_FT + "/tmp/path.txt");
    try (Closeable c = new ConfigurationRule(PropertyKey.ZOOKEEPER_ENABLED, "true")
        .toResource()) {
      final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      Assert.assertTrue(fs instanceof FaultTolerantFileSystem);
    }
  }

  /**
   * Ensures that Hadoop loads the Alluxio file system when configured.
   */
  @Test
  public void hadoopShouldLoadFileSystemWhenConfigured() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();

    URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, uri.getHost(),
        PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()),
        PropertyKey.ZOOKEEPER_ENABLED, "false")).toResource()) {
      final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
      Assert.assertTrue(fs instanceof FileSystem);
    }
  }

  /**
   * Tests that initializing the {@link AbstractFileSystem} will reinitialize contexts to pick up
   * changes to the master address.
   */
  @Test
  public void resetContext() throws Exception {
    // Change to otherhost:410
    URI uri = URI.create(Constants.HEADER + "otherhost:410/");
    org.apache.hadoop.fs.FileSystem fileSystem =
        org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    // Make sure all contexts are using the new address
    InetSocketAddress newAddress = new InetSocketAddress("otherhost", 410);
    Assert.assertEquals(newAddress, CommonTestUtils.getInternalState(LineageContext.INSTANCE,
        "mLineageMasterClientPool", "mMasterAddress"));
  }

  /**
   * Verifies that the initialize method is only called once even when there are many concurrent
   * initializers during the initialization phase.
   */
  @Test
  public void concurrentInitialize() throws Exception {
    List<Thread> threads = new ArrayList<>();
    final org.apache.hadoop.conf.Configuration conf = getConf();
    Mockito.when(mMockFileSystemContext.getMasterAddress())
        .thenReturn(new InetSocketAddress("randomhost", 410));
    for (int i = 0; i < 100; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          URI uri = URI.create(Constants.HEADER + "randomhost:410/");
          try {
            org.apache.hadoop.fs.FileSystem.get(uri, conf);
          } catch (IOException e) {
            Assert.fail();
          }
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
   * Tests that failing to connect to Alluxio master causes initialization failure.
   */
  @Test
  public void initializeFailToConnect() throws Exception {
    Mockito.doThrow(ConnectionFailedException.class)
        .when(mMockFileSystemMasterClient).connect();

    mExpectedException.expect(IOException.class);
    mExpectedException.expectCause(IsInstanceOf.any(ConnectionFailedException.class));

    URI uri = URI.create(Constants.HEADER + "randomhost:400/");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());
    // The above code should throw an exception.
    Assert.fail("Initialization should throw an exception.");
  }

  /**
   * Tests that after initialization, reinitialize with a different URI should fail.
   */
  @Test
  public void reinitializeWithDifferentURI() throws Exception {
    final org.apache.hadoop.conf.Configuration conf = getConf();
    String originalURI = "host1:1";
    URI uri = URI.create(Constants.HEADER + originalURI);
    org.apache.hadoop.fs.FileSystem.get(uri, conf);

    Mockito.when(mMockFileSystemContext.getMasterAddress())
        .thenReturn(new InetSocketAddress("host1", 1));

    String[] newURIs = new String[]{"host2:1", "host1:2", "host2:2"};
    for (String newURI : newURIs) {
      // mExpectedException.expect(IOException.class);
      // mExpectedException.expectMessage(ExceptionMessage.DIFFERENT_MASTER_ADDRESS
      //    .getMessage(newURI, originalURI));

      uri = URI.create(Constants.HEADER + newURI);
      org.apache.hadoop.fs.FileSystem.get(uri, conf);

      // The above code should not throw an exception.
      // TODO(cc): Remove or bring this check back.
      // Assert.fail("Initialization should throw an exception.");
    }
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
        Mockito.mock(alluxio.client.file.FileSystem.class);
    Mockito.when(alluxioFs.listStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
        .thenReturn(Lists.newArrayList(new URIStatus(fileInfo1), new URIStatus(fileInfo2)));
    FileSystem alluxioHadoopFs = new FileSystem(alluxioFs);

    FileStatus[] fileStatuses = alluxioHadoopFs.listStatus(path);
    assertFileInfoEqualsFileStatus(fileInfo1, fileStatuses[0]);
    assertFileInfoEqualsFileStatus(fileInfo2, fileStatuses[1]);
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
        Mockito.mock(alluxio.client.file.FileSystem.class);
    Mockito.when(alluxioFs.getStatus(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path))))
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

  private org.apache.hadoop.conf.Configuration getConf() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (HadoopClientTestUtils.isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", FileSystem.class.getName());
    }
    return conf;
  }

  private void mockFileSystemContextAndMasterClient() throws Exception {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockFileSystemContextCustomized = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.mockStatic(FileSystemContext.class);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mMockFileSystemContext);
    PowerMockito.when(FileSystemContext.create(Mockito.any(Subject.class)))
        .thenReturn(mMockFileSystemContextCustomized);
    mMockFileSystemMasterClient = Mockito.mock(FileSystemMasterClient.class);
    Mockito.when(mMockFileSystemContext.acquireMasterClient())
        .thenReturn(mMockFileSystemMasterClient);
    Mockito.when(mMockFileSystemContextCustomized.acquireMasterClient())
        .thenReturn(mMockFileSystemMasterClient);
    Mockito.doNothing().when(mMockFileSystemMasterClient).connect();
    Mockito.when(mMockFileSystemContext.getMasterAddress())
        .thenReturn(new InetSocketAddress("defaultHost", 1));
  }

  private void mockUserGroupInformation(String username) throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    Mockito.when(ugi.getUserName()).thenReturn(username);
    Mockito.when(ugi.getShortUserName()).thenReturn(username.split("@")[0]);
  }

  private void assertFileInfoEqualsFileStatus(FileInfo info, FileStatus status) {
    Assert.assertEquals(info.getOwner(), status.getOwner());
    Assert.assertEquals(info.getGroup(), status.getGroup());
    Assert.assertEquals(info.getMode(), status.getPermission().toShort());
    Assert.assertEquals(info.getLastModificationTimeMs(), status.getModificationTime());
    Assert.assertEquals(info.isFolder(), status.isDir());
  }
}
