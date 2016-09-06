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

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.lineage.LineageContext;
import alluxio.client.util.ClientTestUtils;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
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
import java.util.ArrayList;
import java.util.List;

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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Sets up the configuration before a test runs.
   */
  @Before
  public void before() throws Exception {
    mockUserGroupInformation();

    if (isHadoop1x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 1x");
    } else if (isHadoop2x()) {
      LOG.debug("Running Alluxio FS tests against hadoop 2x");
    } else {
      LOG.warn("Running Alluxio FS tests against untargeted Hadoop version: " + getHadoopVersion());
    }
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
    ClientTestUtils.resetClient();
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
   */
  @Test
  public void hadoopShouldLoadFaultTolerantFileSystemWhenConfigured() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", FaultTolerantFileSystem.class.getName());
    }

    URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, "true");

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
    Assert.assertTrue(fs instanceof FaultTolerantFileSystem);
  }

  /**
   * Ensures that Hadoop loads the Alluxio file system when configured.
   */
  @Test
  public void hadoopShouldLoadFileSystemWhenConfigured() throws Exception {
    org.apache.hadoop.conf.Configuration conf = getConf();

    URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, "false");

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);
    Assert.assertTrue(fs instanceof FileSystem);
  }

  /**
   * Tests that initializing the {@link AbstractFileSystem} will reinitialize contexts to pick up
   * changes to the master address.
   */
  @Test
  public void resetContext() throws Exception {
    FileSystemContext fileSystemContext = PowerMockito.mock(FileSystemContext.class);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", fileSystemContext);

    // Change to otherhost:410
    URI uri = URI.create(Constants.HEADER + "otherhost:410/");
    org.apache.hadoop.fs.FileSystem.get(uri, getConf());

    // Make sure all contexts are using the new address
    InetSocketAddress newAddress = new InetSocketAddress("otherhost", 410);
    Assert.assertEquals(newAddress, ClientContext.getMasterAddress());
    Assert.assertEquals(newAddress, CommonTestUtils.getInternalState(BlockStoreContext.get(),
        "mBlockMasterClientPool", "mMasterAddress"));
    Mockito.verify(fileSystemContext).reset();
    Assert.assertEquals(newAddress, CommonTestUtils.getInternalState(LineageContext.INSTANCE,
        "mLineageMasterClientPool", "mMasterAddress"));
  }

  /**
   * Verifies that the initialize method is only called once even when there are many concurrent
   * initializers during the initialization phase.
   */
  @Test
  public void concurrentInitialize() throws Exception {
    FileSystemContext fileSystemContext = PowerMockito.mock(FileSystemContext.class);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", fileSystemContext);

    List<Thread> threads = new ArrayList<>();
    final org.apache.hadoop.conf.Configuration conf = getConf();
    for (int i = 0; i < 100; i++) {
      final int id = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          URI uri = URI.create(Constants.HEADER + "randomhost" + id + ":410/");
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
    Mockito.verify(fileSystemContext).reset();
  }

  private boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  private boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
  }

  private org.apache.hadoop.conf.Configuration getConf() throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", FileSystem.class.getName());
    }
    return conf;
  }

  private void mockUserGroupInformation() throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
  }
}
