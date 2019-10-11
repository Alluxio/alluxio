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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link UdbConfiguration}.
 */
public class UdbConfigurationTest {

  @Test
  public void mountOptions() {
    testMountOptions("SCHEME" + randomString(), true);
    testMountOptions("SCHEME" + randomString(), false);
  }

  private void testMountOptions(String schemeAuthority, boolean specifyTrailingSlash) {
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      values.put("PROPERTY" + randomString(), "VALUE" + randomString());
    }

    Map<String, String> properties = new HashMap<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      String schemeOption = String.format("{%s}", schemeAuthority);
      if (specifyTrailingSlash) {
        schemeOption = String.format("{%s/}", schemeAuthority);
      }
      properties.put(
          String.format("%s%s.%s", ConfigurationUtils.MOUNT_PREFIX, schemeOption, entry.getKey()),
          entry.getValue());
    }
    UdbConfiguration conf = new UdbConfiguration(properties);

    // query for mount options with and without the trailing slash
    assertEquals(values, conf.getMountOption(schemeAuthority));
    assertEquals(values, conf.getMountOption(schemeAuthority + "/"));
  }

  private String randomString() {
    List<String> parts = new ArrayList<>();
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 4); i++) {
      parts.add(CommonUtils.randomAlphaNumString(5));
    }
    return String.join(".", parts);
  }
}
