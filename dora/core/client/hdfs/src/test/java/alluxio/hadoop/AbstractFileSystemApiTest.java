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

import alluxio.TestLoggerRule;
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Ignore;
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

  private InstancedConfiguration mConf = Configuration.copyGlobal();

  @After
  public void after() {
    mConf = Configuration.copyGlobal();
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
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "Jiacheng",
      comment = "fix test because the URI is general")
  @Ignore
  public void noAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio:///");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(loggedAuthorityWarning());
  }

  @Test
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "Jiacheng",
      comment = "fix test because the URI is general")
  @Ignore
  public void validAuthorityNoWarning() throws IOException {
    URI unknown = URI.create("alluxio://localhost:12345/");
    FileSystem.get(unknown, new org.apache.hadoop.conf.Configuration());
    assertFalse(loggedAuthorityWarning());
  }

  private boolean loggedAuthorityWarning() {
    return mTestLogger.wasLogged("Authority .* is unknown");
  }
}

