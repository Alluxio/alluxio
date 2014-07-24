/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.MockClassLoader;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.Constants;
import tachyon.client.TachyonFS;

/**
 * Unit tests for TFS
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TachyonFS.class, UserGroupInformation.class })
public class TFSTest {

  private final Logger LOG = Logger.getLogger(TFSTest.class.getName());

  @Before
  public void setup() throws Exception {
    mockUserGroupInformation();
    mockTachyonFSGet();

    if (isHadoop1x()) {
      LOG.debug("Running TFS tests against hadoop 1x");
    } else if (isHadoop2x()) {
      LOG.debug("Running TFS tests against hadoop 2x");
    } else {
      LOG.warn("Running TFS tests against untargeted Hadoop version: " + getHadoopVersion());
    }
  }

  @Test
  public void hadoopShouldLoadTfsWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", TFS.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER + "localhost:19998/tmp/path.txt");
    final FileSystem fs = FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFS);

    PowerMockito.verifyStatic();
    TachyonFS.get("localhost", 19998, false);
  }

  @Test
  public void hadoopShouldLoadTfsFtWhenConfigured() throws IOException {
    final Configuration conf = new Configuration();
    if (isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME_FT + ".impl", TFSFT.class.getName());
    }

    // when
    final URI uri = URI.create(Constants.HEADER_FT + "localhost:19998/tmp/path.txt");
    final FileSystem fs = FileSystem.get(uri, conf);

    Assert.assertTrue(fs instanceof TFSFT);

    PowerMockito.verifyStatic();
    TachyonFS.get("localhost", 19998, true);
  }

  private void mockTachyonFSGet() throws IOException {
    mockStatic(TachyonFS.class);
    TachyonFS tachyonFS = mock(TachyonFS.class);
    when(TachyonFS.get(anyString(), anyInt(), anyBoolean())).thenReturn(tachyonFS);
  }

  private void mockUserGroupInformation() throws IOException {
    // need to mock out since FileSystem.get calls UGI, which some times has issues on some systems
    mockStatic(UserGroupInformation.class);
    final UserGroupInformation ugi = mock(UserGroupInformation.class);
    when(ugi.getCurrentUser()).thenReturn(ugi);
  }

  private boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  private boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
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

  private ClassLoader getClassLoader(Class<?> clazz) {
    // Power Mock makes this hard, so try to hack it
    ClassLoader cl = clazz.getClassLoader();
    if (cl instanceof MockClassLoader) {
      cl = cl.getParent();
    }
    return cl;
  }
}
