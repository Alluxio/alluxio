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

package alluxio.master.meta.checkconf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.grpc.ConfigProperty;
import alluxio.wire.Address;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Unit tests for {@link ServerConfigurationStore}.
 */
public class ServerConfigurationStoreTest {
  private List<ConfigProperty> mConfigListOne;
  private List<ConfigProperty> mConfigListTwo;
  private Address mAddressOne;
  private Address mAddressTwo;

  @Before
  public void before() {
    PropertyKey keyEnforce = PropertyKey.ZOOKEEPER_ELECTION_PATH;
    PropertyKey keyWarn = PropertyKey.WORKER_FREE_SPACE_TIMEOUT;
    mConfigListOne = Arrays.asList(
        ConfigProperty.newBuilder().setName(keyEnforce.getName()).setSource("Test")
            .setValue("Value").build(),
        ConfigProperty.newBuilder().setName(keyWarn.getName()).setSource("Test").setValue("Value2")
            .build());
    mConfigListTwo = Arrays.asList(
        ConfigProperty.newBuilder().setName(keyEnforce.getName()).setSource("Test")
            .setValue("Value3").build(),
        ConfigProperty.newBuilder().setName(keyWarn.getName()).setSource("Test").setValue("Value4")
            .build());
    Random random = new Random();
    mAddressOne = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());
    mAddressTwo = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());
  }

  @Test
  public void registerNewConf() {
    ServerConfigurationStore configStore = createConfigStore();

    Map<Address, List<ConfigRecord>> confMap = configStore.getConfMap();

    assertTrue(confMap.containsKey(mAddressOne));
    assertTrue(confMap.containsKey(mAddressTwo));
  }

  @Test
  public void registerNewConfUnknownProperty() {
    Address testAddress = new Address("test", 0);
    ServerConfigurationStore configStore = new ServerConfigurationStore();
    configStore.registerNewConf(testAddress,
        Arrays.asList(ConfigProperty.newBuilder().setName("unknown.property").build()
    ));
    Map<Address, List<ConfigRecord>> confMap = configStore.getConfMap();
    assertTrue(confMap.containsKey(testAddress));
    assertEquals("unknown.property", confMap.get(testAddress).get(0).getKey().getName());
  }

  @Test
  public void detectNodeLost() {
    ServerConfigurationStore configStore = createConfigStore();

    configStore.handleNodeLost(mAddressOne);

    Map<Address, List<ConfigRecord>> confMap = configStore.getConfMap();

    assertFalse(confMap.containsKey(mAddressOne));
    assertTrue(confMap.containsKey(mAddressTwo));
  }

  @Test
  public void lostNodeFound() {
    ServerConfigurationStore configStore = createConfigStore();

    configStore.handleNodeLost(mAddressOne);
    configStore.handleNodeLost(mAddressTwo);

    Map<Address, List<ConfigRecord>> confMap = configStore.getConfMap();
    assertFalse(confMap.containsKey(mAddressOne));
    assertFalse(confMap.containsKey(mAddressTwo));

    configStore.lostNodeFound(mAddressTwo);
    confMap = configStore.getConfMap();

    assertFalse(confMap.containsKey(mAddressOne));
    assertTrue(confMap.containsKey(mAddressTwo));
  }

  /**
   * @return a config store with two conf registered
   */
  private ServerConfigurationStore createConfigStore() {
    ServerConfigurationStore configStore = new ServerConfigurationStore();
    configStore.registerNewConf(mAddressOne, mConfigListOne);
    configStore.registerNewConf(mAddressTwo, mConfigListTwo);
    return configStore;
  }
}
