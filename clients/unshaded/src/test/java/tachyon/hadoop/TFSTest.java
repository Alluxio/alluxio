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

package tachyon.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.MockClassLoader;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;

/**
 * Unit tests for TFS
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TachyonFS.class, UserGroupInformation.class})
public class TFSTest {
  private static final Logger LOG = LoggerFactory.getLogger(TFSTest.class.getName());

  private TachyonConf mTachyonConf;

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
      final URL url = getSourcePath(FileSystem.class);
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

  @Test
  public void hadoopShouldLoadTfsFtWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", TFSFT.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");

    mTachyonConf.set(Constants.MASTER_HOSTNAME, uri.getHost());
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(uri.getPort()));
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "true");
    mockTachyonFSGet();

    final FileSystem fs = FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFSFT);

    PowerMockito.verifyStatic();
    TachyonFS.get(mTachyonConf);
  }

  @Test
  public void hadoopShouldLoadTfsWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", TFS.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");

    mTachyonConf.set(Constants.MASTER_HOSTNAME, uri.getHost());
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(uri.getPort()));
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false");
    mockTachyonFSGet();

    final FileSystem fs = FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFS);

    PowerMockito.verifyStatic();
    TachyonFS.get(mTachyonConf);
  }

  private boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  private boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
  }

  private void mockTachyonFSGet() throws IOException {
    PowerMockito.mockStatic(TachyonFS.class);
    TachyonFS tachyonFS = Mockito.mock(TachyonFS.class);
    Mockito.when(TachyonFS.get(Matchers.eq(mTachyonConf))).thenReturn(tachyonFS);
  }

  private void mockUserGroupInformation() throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    PowerMockito.mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(ugi.getCurrentUser()).thenReturn(ugi);
  }

  @Before
  public void setup() throws Exception {
    mTachyonConf = new TachyonConf();
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
