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

package alluxio.master.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.util.IdUtils;
import alluxio.wire.ConfigProperty;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ServerConfigurationChecker}.
 */
public class ServerConfigurationCheckerTest {
  private List<ConfigProperty> mConfigListOne;
  private List<ConfigProperty> mConfigListTwo;
  private long mIdOne;
  private long mIdTwo;

  @Before
  public void before() {
    mConfigListOne = Arrays.asList(
        new ConfigProperty().setName("TestName").setSource("TestSource").setValue("TestValue"),
        new ConfigProperty().setName("TestName2").setSource("TestSource2").setValue("TestValue2"));
    mConfigListTwo = Arrays.asList(
        new ConfigProperty().setName("TestName3").setSource("TestSource3").setValue("TestValue3"),
        new ConfigProperty().setName("TestName4").setSource("TestSource4").setValue("TestValue4"));
    mIdOne = IdUtils.getRandomNonNegativeLong();
    mIdTwo = IdUtils.getRandomNonNegativeLong();
  }

  @Test
  public void registerNewConf() {
    ServerConfigurationChecker configChecker = createConfigChecker();

    Map<Long, List<ConfigProperty>> confMap = configChecker.getConfMap();

    assertTrue(confMap.containsKey(mIdOne));
    assertTrue(confMap.containsKey(mIdTwo));

    assertEquals(mConfigListOne, confMap.get(mIdOne));
    assertEquals(mConfigListTwo, confMap.get(mIdTwo));
  }

  @Test
  public void detectNodeLost() {
    ServerConfigurationChecker configChecker = createConfigChecker();

    configChecker.handleNodeLost(mIdOne);

    Map<Long, List<ConfigProperty>> confMap = configChecker.getConfMap();

    assertFalse(confMap.containsKey(mIdOne));
    assertTrue(confMap.containsKey(mIdTwo));
  }

  @Test
  public void lostNodeFound() {
    ServerConfigurationChecker configChecker = createConfigChecker();

    configChecker.handleNodeLost(mIdOne);
    configChecker.handleNodeLost(mIdTwo);

    Map<Long, List<ConfigProperty>> confMap = configChecker.getConfMap();
    assertFalse(confMap.containsKey(mIdTwo));
    assertFalse(confMap.containsKey(mIdOne));

    configChecker.lostNodeFound(mIdTwo);
    confMap = configChecker.getConfMap();

    assertTrue(confMap.containsKey(mIdTwo));
    assertEquals(mConfigListTwo, confMap.get(mIdTwo));
    assertFalse(confMap.containsKey(mIdOne));
  }

  /**
   * @return a config checker with two conf registered
   */
  private ServerConfigurationChecker createConfigChecker() {
    ServerConfigurationChecker configChecker = new ServerConfigurationChecker();
    configChecker.registerNewConf(mIdOne, mConfigListOne);
    configChecker.registerNewConf(mIdTwo, mConfigListTwo);
    return configChecker;
  }
}
