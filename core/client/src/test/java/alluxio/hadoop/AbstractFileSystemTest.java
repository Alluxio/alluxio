/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.CommonTestUtils;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.lineage.LineageContext;
import alluxio.client.util.ClientTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.MockClassLoader;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Unit tests for {@link FileSystem}.
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private FileSystemContext mMockFileSystemContext;

  /**
   * Sets up the configuration before a test runs.
   *
   * @throws Exception when creating the mock fails
   */
  @Before
  public void setup() throws Exception {
    mockUserGroupInformation();
    mockMasterClient();

    if (isHadoop1x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 1x");
    } else if (isHadoop2x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 2x");
    } else {
      LOG.warn("Running Alluxio FS tests against untargeted Hadoop version: " + getHadoopVersion());
    }
  }

  private ClassLoader getClassLoader(Class<?> clazz) {
    // Power Mock makes this hard, so try to hack it
    ClassLoader cl = clazz.getClassLoader();
    if (cl instanceof MockClassLoader) {
      cl = cl.getParent();
    }
    return cl;
  }

  private String getHadoopVersion() {
    try {
      final URL url = getSourcePath(org.apache.hadoop.fs.FileSystem.class);
      final File path = new File(url.toURI());
      final String[] splits = path.getName().split("-");
      final String last = splits[splits.length - 1];
      return last.substring(0, last.lastIndexOf("."));
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  private URL getSourcePath(Class<?> clazz) {
    try {
      clazz = getClassLoader(clazz).loadClass(clazz.getName());
      return clazz.getProtectionDomain().getCodeSource().getLocation();
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Unable to find class " + clazz.getName());
    }
  }

  /**
   * Ensures that Hadoop loads {@link FaultTolerantFileSystem} when configured.
   *
   * @throws IOException when the file system cannot be retrieved
   */
  @Test
  public void hadoopShouldLoadFaultTolerantFileSystemWhenConfiguredTest() throws Exception {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", FaultTolerantFileSystem.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, uri.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    ClientContext.getConf().set(Constants.ZOOKEEPER_ENABLED, "true");

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof FaultTolerantFileSystem);

    PowerMockito.verifyStatic();
    alluxio.client.file.FileSystem.Factory.get();
    ClientTestUtils.resetClientContext();
  }

  /**
   * Ensures that Hadoop loads the Alluxio file system when configured.
   */
  @Test
  public void hadoopShouldLoadFileSystemWhenConfiguredTest() throws Exception {
    final Configuration conf = getConf();

    // when
    final URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, uri.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    ClientContext.getConf().set(Constants.ZOOKEEPER_ENABLED, "false");

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof FileSystem);

    PowerMockito.verifyStatic();
    alluxio.client.file.FileSystem.Factory.get();
    ClientTestUtils.resetClientContext();
  }

  /**
   * Tests that initializing the {@link AbstractFileSystem} will reinitialize contexts to pick up
   * changes to the master address.
   */
  @Test
  public void resetContextTest() throws Exception {
    // Create system with master at localhost:19998
    URI uri = URI.create(Constants.HEADER + "localhost:19998/");
    Configuration conf = getConf();
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    // Change to otherhost:410
    URI newUri = URI.create(Constants.HEADER + "otherhost:410/");
    fs.initialize(newUri, conf);

    // Make sure all contexts are using the new address
    InetSocketAddress newAddress = new InetSocketAddress("otherhost", 410);
    Assert.assertEquals(newAddress, ClientContext.getMasterAddress());
    Assert.assertEquals(newAddress, CommonTestUtils.getInternalState(BlockStoreContext.INSTANCE,
        "mBlockMasterClientPool", "mMasterAddress"));
    // Once from calling FileSystem.get, once from calling initialize.
    Mockito.verify(mMockFileSystemContext, Mockito.times(2)).reset();
    Assert.assertEquals(newAddress, CommonTestUtils.getInternalState(LineageContext.INSTANCE,
        "mLineageMasterClientPool", "mMasterAddress"));
  }

  private boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  private boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
  }

  private Configuration getConf() throws Exception {
    Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", FileSystem.class.getName());
    }
    return conf;
  }

  private void mockMasterClient() {
    PowerMockito.mockStatic(FileSystemContext.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    FileSystemMasterClient mockMaster =
        PowerMockito.mock(FileSystemMasterClient.class);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mMockFileSystemContext);
    Mockito.when(mMockFileSystemContext.acquireMasterClient()).thenReturn(mockMaster);
  }

  private void mockUserGroupInformation() throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
  }
}
