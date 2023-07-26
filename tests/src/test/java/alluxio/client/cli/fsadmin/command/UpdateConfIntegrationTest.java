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

package alluxio.client.cli.fsadmin.command;

import alluxio.cli.fsadmin.command.UpdateConfCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for updateConf command.
 */
public final class UpdateConfIntegrationTest extends AbstractFsAdminShellTest {

  @Test
  public void unknownOption() {
    int ret = mFsAdminShell.run("updateConf", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(UpdateConfCommand.description(), lastLine(output));
  }

  @Test
  public void updateUnknownKey() {
    int ret = mFsAdminShell.run("updateConf", "unknown-key=unknown-value");
    Assert.assertEquals(-2, ret);
    ret = mFsAdminShell.run("updateConf", "unknown-key");
    Assert.assertEquals(-1, ret);
    ret = mFsAdminShell.run("updateConf", "unknown-key=1=2");
    Assert.assertEquals(-3, ret);
  }

  @Test
  public void updateNormal() {
    int ret = mFsAdminShell.run("updateConf", "alluxio.master.worker.timeout=4min");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void updateNonDynamicKey() {
    int ret = mFsAdminShell.run("updateConf",
        "alluxio.security.authorization.permission.enabled=false");
    Assert.assertEquals(-2, ret);
  }

  @Test
  public void getUpdatedConf() {
    long t1 = System.nanoTime();
    Configuration.updateConfiguration(Collections.singletonMap("key1", "value1"), true);
    long t2 = System.nanoTime();
    Configuration.updateConfiguration(Collections.singletonMap("key2", "value2"), true);
    long t3 = System.nanoTime();
    Configuration.updateConfiguration(Collections.singletonMap("key3", "value3"), true);
    Pair<List<Map<String, String>>, Long> pair = Configuration.getUpdatedConfigs(t1);
    Assert.assertEquals(3, pair.getFirst().size());
    Assert.assertTrue(pair.getSecond() > t3);
    long t4 = System.nanoTime();
    Configuration.updateConfiguration(Collections.singletonMap("key3", "value3new"), true);
    Assert.assertEquals(3, Configuration.getUpdatedConfigs(t2).getFirst().size());
    Assert.assertEquals(2, Configuration.getUpdatedConfigs(t3).getFirst().size());
    Assert.assertEquals(1, Configuration.getUpdatedConfigs(t4).getFirst().size());
    Assert.assertEquals(0, Configuration.getUpdatedConfigs(System.nanoTime()).getFirst().size());
  }

  @Test
  public void updatedConfMapOverflowTest() {
    Configuration.clearUpdatedConfigMap();
    for (int i = 0; i < 500; i++) {
      Configuration.updateConfiguration(Collections.singletonMap("key", "value"), true);
      Assert.assertEquals(i + 1, Configuration.getUpdatedConfigMapSize());
    }
    for (int i = 0; i < 500; i++) {
      Configuration.updateConfiguration(Collections.singletonMap("key", "value"), true);
      Assert.assertEquals(500, Configuration.getUpdatedConfigMapSize());
    }
  }

  private String lastLine(String output) {
    String[] lines = output.split("\n");
    if (lines.length > 0) {
      return lines[lines.length - 1];
    }
    return "";
  }
}
