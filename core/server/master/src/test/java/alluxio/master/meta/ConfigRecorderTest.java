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

import alluxio.util.IdUtils;
import alluxio.wire.ConfigProperty;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ConfigRecorder}.
 */
public class ConfigRecorderTest {
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
    ConfigRecorder configRecorder = createConfigRecorder();

    Map<Long, List<ConfigProperty>> confMap = configRecorder.getConfMap();

    Assert.assertTrue(confMap.containsKey(mIdOne));
    Assert.assertTrue(confMap.containsKey(mIdTwo));

    Assert.assertEquals(mConfigListOne, confMap.get(mIdOne));
    Assert.assertEquals(mConfigListTwo, confMap.get(mIdTwo));
  }

  @Test
  public void detectNodeLost() {
    ConfigRecorder configRecorder = createConfigRecorder();

    configRecorder.detectNodeLost(mIdOne);

    Map<Long, List<ConfigProperty>> confMap = configRecorder.getConfMap();

    Assert.assertFalse(confMap.containsKey(mIdOne));
    Assert.assertTrue(confMap.containsKey(mIdTwo));
  }

  @Test
  public void lostNodeFound() {
    ConfigRecorder configRecorder = createConfigRecorder();

    configRecorder.detectNodeLost(mIdOne);
    configRecorder.detectNodeLost(mIdTwo);

    Map<Long, List<ConfigProperty>> confMap = configRecorder.getConfMap();
    Assert.assertFalse(confMap.containsKey(mIdTwo));
    Assert.assertFalse(confMap.containsKey(mIdOne));

    configRecorder.lostNodeFound(mIdTwo);
    confMap = configRecorder.getConfMap();

    Assert.assertTrue(confMap.containsKey(mIdTwo));
    Assert.assertEquals(mConfigListTwo, confMap.get(mIdTwo));
    Assert.assertFalse(confMap.containsKey(mIdOne));
  }

  /**
   * @return a config recorder with two conf registered
   */
  private ConfigRecorder createConfigRecorder() {
    ConfigRecorder configRecorder = new ConfigRecorder();
    configRecorder.registerNewConf(mIdOne, mConfigListOne);
    configRecorder.registerNewConf(mIdTwo, mConfigListTwo);
    return configRecorder;
  }
}
