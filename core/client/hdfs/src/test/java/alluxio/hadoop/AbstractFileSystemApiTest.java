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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.TestLoggerRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Tests for {@link AbstractFileSystem}. Unlike {@link AbstractFileSystemTest}, these tests only
 * exercise the public API of {@link AbstractFileSystem}.
 */
public final class AbstractFileSystemApiTest {

  @Rule
  public TestLoggerRule mTestLogger = new TestLoggerRule();

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    // To make the test run faster.
    mConf.set(PropertyKey.METRICS_CONTEXT_SHUTDOWN_TIMEOUT, "0sec");
  }

  @After
  public void after() {
    mConf = ConfigurationTestUtils.defaults();
    HadoopClientTestUtils.disableMetrics(mConf);
  }

  @Test
  public void unknownAuthorityTriggersWarning() throws IOException {
    URI unknown = URI.create("alluxio://test/");
    Exception e = assertThrows(Exception.class, () ->
        FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration()));
    assertTrue(e.getMessage().contains("Authority \"test\" is unknown. "
        + "The client can not be configured with the authority from " + unknown));
  }

  @Test
  public void noAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio:///");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(loggedAuthorityWarning());
  }

  @Test
  public void validAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio://localhost:12345/");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(loggedAuthorityWarning());
  }

  @Test
  public void parseZkUriWithPlusDelimiters() throws Exception {
    org.apache.hadoop.fs.FileSystem fs = FileSystem.get(URI.create("alluxio://zk@a:0+b:1+c:2/"),
        new org.apache.hadoop.conf.Configuration());
    assertTrue(fs instanceof AbstractFileSystem);
    AbstractFileSystem afs = (AbstractFileSystem) fs;
    assertTrue(afs.mFileSystem.getConf()
        .getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    assertEquals("a:0,b:1,c:2", afs.mFileSystem.getConf()
        .get(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  private boolean loggedAuthorityWarning() {
    return mTestLogger.wasLogged("Authority .* is unknown");
  }
}

