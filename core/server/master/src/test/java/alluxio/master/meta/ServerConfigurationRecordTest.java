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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.PropertyKey;
import alluxio.PropertyKey.Builder;
import alluxio.PropertyKey.ConsistencyCheckLevel;
import alluxio.master.meta.checkconf.ConfigRecord;
import alluxio.master.meta.checkconf.ServerConfigurationRecord;
import alluxio.wire.ConfigProperty;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ServerConfigurationRecord}.
 */
public class ServerConfigurationRecordTest {
  private List<ConfigProperty> mConfigListOne;
  private List<ConfigProperty> mConfigListTwo;
  private String mHostOne;
  private String mHostTwo;

  @Before
  public void before() {
    PropertyKey keyEnforce = new Builder("TestKey1")
        .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE).build();
    PropertyKey keyWarn = new Builder("TestKey2")
        .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN).build();
    mConfigListOne = Arrays.asList(
        new ConfigProperty().setName(keyEnforce.getName()).setSource("Test").setValue("Value"),
        new ConfigProperty().setName(keyWarn.getName()).setSource("Test").setValue("Value2"));
    mConfigListTwo = Arrays.asList(
        new ConfigProperty().setName(keyEnforce.getName()).setSource("Test").setValue("Value3"),
        new ConfigProperty().setName(keyWarn.getName()).setSource("Test").setValue("Value4"));
    mHostOne = RandomStringUtils.randomAlphanumeric(10);
    mHostTwo = RandomStringUtils.randomAlphanumeric(10);
  }

  @Test
  public void registerNewConf() {
    ServerConfigurationRecord configRecord = createConfigRecord();

    Map<String, List<ConfigRecord>> confMap = configRecord.getConfMap();

    assertTrue(confMap.containsKey(mHostOne));
    assertTrue(confMap.containsKey(mHostTwo));
  }

  @Test
  public void detectNodeLost() {
    ServerConfigurationRecord configRecord = createConfigRecord();

    configRecord.handleNodeLost(mHostOne);

    Map<String, List<ConfigRecord>> confMap = configRecord.getConfMap();

    assertFalse(confMap.containsKey(mHostOne));
    assertTrue(confMap.containsKey(mHostTwo));
  }

  @Test
  public void lostNodeFound() {
    ServerConfigurationRecord configRecord = createConfigRecord();

    configRecord.handleNodeLost(mHostOne);
    configRecord.handleNodeLost(mHostTwo);

    Map<String, List<ConfigRecord>> confMap = configRecord.getConfMap();
    assertFalse(confMap.containsKey(mHostTwo));
    assertFalse(confMap.containsKey(mHostOne));

    configRecord.lostNodeFound(mHostTwo);
    confMap = configRecord.getConfMap();

    assertTrue(confMap.containsKey(mHostTwo));
    assertFalse(confMap.containsKey(mHostOne));
  }

  /**
   * @return a config record with two conf registered
   */
  private ServerConfigurationRecord createConfigRecord() {
    ServerConfigurationRecord configRecord = new ServerConfigurationRecord();
    configRecord.registerNewConf(mHostOne, mConfigListOne);
    configRecord.registerNewConf(mHostTwo, mConfigListTwo);
    return configRecord;
  }
}
