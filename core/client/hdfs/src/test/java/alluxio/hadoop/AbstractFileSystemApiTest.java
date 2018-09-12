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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.TestLoggerRule;

import org.junit.After;
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

  @After
  public void after() {
    HadoopClientTestUtils.resetClient();
  }

  @Test
  public void unknownAuthorityTriggersWarning() throws IOException {
    URI unknown = URI.create("alluxio://test/");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertTrue(mTestLogger.wasLogged("Authority \"test\" is unknown"));
  }

  @Test
  public void noAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio:///");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(mTestLogger.wasLogged("Authority \"\" is unknown"));
  }

  @Test
  public void validAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio://localhost:12345/");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(mTestLogger.wasLogged("Authority \"localhost:12345\" is unknown"));
  }
}
