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

package alluxio.table.common.udb;

import static org.junit.Assert.assertEquals;

import alluxio.table.common.ConfigurationUtils;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class UdbConfigurationTest {

  @Test
  public void multipleUfsMountOptions() {
    Map<String, String> opts = new ImmutableMap.Builder<String, String>()
        .put("my.special.key", "myspecialvalue")
        .put(ConfigurationUtils.MOUNT_PREFIX + "{ufs://a.a}.key1", "v1")
        .put(ConfigurationUtils.MOUNT_PREFIX + "{ufs://a.a}.key2", "v2")
        .put(ConfigurationUtils.MOUNT_PREFIX + "{ufs://b.b}.key2", "v3")
        .put(ConfigurationUtils.MOUNT_PREFIX + "{file}.key2", "v4")
        .build();

    UdbConfiguration conf = new UdbConfiguration(opts);
    assertEquals(3, Whitebox.<Map<String, String>>getInternalState(conf, "mMountOptions").size());
    assertEquals(0, conf.getMountOption("").size());
    assertEquals(1, conf.getMountOption("ufs://b.b").size());
    assertEquals(2, conf.getMountOption("ufs://a.a").size());
    assertEquals(1, conf.getMountOption("file").size());
  }

  @Test
  public void mountOptions() {
    testMountOptions("SCHEME" + randomString(), true);
    testMountOptions("SCHEME" + randomString(), false);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + ".*", "SCHEME" + randomString(),
        true, true);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + ".*", "SCHEME" + randomString(),
        false, true);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + "SCHE.E.*", "SCHEME" + randomString(),
        true, true);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + "SCHE.E.*", "SCHEME" + randomString(),
        false, true);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + "SCHEME1.*", "SCHEME2" + randomString(),
        true, false);
    testMountOptions(UdbConfiguration.REGEX_PREFIX + "SCHEME1.*", "SCHEME2" + randomString(),
        false, false);
  }

  private void testMountOptions(String concreteSchemeAuthority, boolean specifyTrailingSlash) {
    testMountOptions(concreteSchemeAuthority, concreteSchemeAuthority, specifyTrailingSlash, true);
  }

  private void testMountOptions(String templateSchemeAuthority, String concreteSchemeAuthority,
      boolean specifyTrailingSlash, boolean expectedExist) {
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      values.put("PROPERTY" + randomString(), "VALUE" + randomString());
    }

    Map<String, String> properties = new HashMap<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      String schemeOption = String.format("{%s}", templateSchemeAuthority);
      if (specifyTrailingSlash) {
        schemeOption = String.format("{%s/}", templateSchemeAuthority);
      }
      properties.put(
          String.format("%s%s.%s", ConfigurationUtils.MOUNT_PREFIX, schemeOption, entry.getKey()),
          entry.getValue());
    }
    UdbConfiguration conf = new UdbConfiguration(properties);

    // query for mount options with and without the trailing slash
    if (!expectedExist) {
      values = Collections.emptyMap();
    }
    assertEquals(values, conf.getMountOption(concreteSchemeAuthority));
    assertEquals(values, conf.getMountOption(concreteSchemeAuthority + "/"));
  }

  private String randomString() {
    List<String> parts = new ArrayList<>();
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 4); i++) {
      parts.add(CommonUtils.randomAlphaNumString(5));
    }
    return String.join(".", parts);
  }
}
