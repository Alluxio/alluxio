/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

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

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.util.ClientTestUtils;

/**
 * Unit tests for {@link TFS}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, UserGroupInformation.class})
/*
 * [TACHYON-1384] Tell PowerMock to defer the loading of javax.security classes to the system
 * classloader in order to avoid linkage error when running this test with CDH.
 * See https://code.google.com/p/powermock/wiki/FAQ.
 */
@PowerMockIgnore("javax.security.*")
public class TFSTest {
  private static final Logger LOG = LoggerFactory.getLogger(TFSTest.class.getName());

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
   * Ensures that Hadoop loads TFSFT when configured.
   *
   * @throws IOException when the file system cannot be retrieved
   */
  @Test
  public void hadoopShouldLoadTfsFtWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", TFSFT.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, uri.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    ClientContext.getConf().set(Constants.ZOOKEEPER_ENABLED, "true");
    mockMasterClient();

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFSFT);

    PowerMockito.verifyStatic();
    FileSystem.Factory.get();
    ClientTestUtils.resetClientContext();
  }

  /**
   * Ensures that Hadoop loads the Tachyon file system when configured.
   *
   * @throws IOException when the file system cannot be retrieved
   */
  @Test
  public void hadoopShouldLoadTfsWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", TFS.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, uri.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    ClientContext.getConf().set(Constants.ZOOKEEPER_ENABLED, "false");
    mockMasterClient();

    final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFS);

    PowerMockito.verifyStatic();
    FileSystem.Factory.get();
    ClientTestUtils.resetClientContext();
  }

  private boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  private boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
  }

  private void mockMasterClient() {
    PowerMockito.mockStatic(FileSystemContext.class);
    FileSystemContext mockContext = PowerMockito.mock(FileSystemContext.class);
    FileSystemMasterClient mockMaster =
        PowerMockito.mock(FileSystemMasterClient.class);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mockContext);
    Mockito.when(mockContext.acquireMasterClient()).thenReturn(mockMaster);
  }

  private void mockUserGroupInformation() throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
  }

  /**
   * Sets up the configuration before a test runs.
   *
   * @throws Exception when creating the mock fails
   */
  @Before
  public void setup() throws Exception {
    mockUserGroupInformation();

    if (isHadoop1x()) {
      LOG.debug("Running TFS tests against hadoop 1x");
    } else if (isHadoop2x()) {
      LOG.debug("Running TFS tests against hadoop 2x");
    } else {
      LOG.warn("Running TFS tests against untargeted Hadoop version: " + getHadoopVersion());
    }
  }
}
