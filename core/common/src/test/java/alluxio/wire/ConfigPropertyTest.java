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

package alluxio.wire;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ConfigPropertyTest {

  @Test
  public void json() throws Exception {
    ConfigProperty configProperty = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    ConfigProperty other =
        mapper.readValue(mapper.writeValueAsBytes(configProperty), ConfigProperty.class);
    checkEquality(configProperty, other);
  }

  @Test
  public void thrift() {
    ConfigProperty configProperty = createRandom();
    ConfigProperty other = ConfigProperty.fromThrift(configProperty.toThrift());
    checkEquality(configProperty, other);
  }

  private void checkEquality(ConfigProperty a, ConfigProperty b) {
    Assert.assertEquals(a.getName(), b.getName());
    Assert.assertEquals(a.getSource(), b.getSource());
    Assert.assertEquals(a.getValue(), b.getValue());
    Assert.assertEquals(a, b);
  }

  private static ConfigProperty createRandom() {
    ConfigProperty result = new ConfigProperty();
    Random random = new Random();

    String name = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String source = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String value = CommonUtils.randomAlphaNumString(random.nextInt(10));

    result.setName(name);
    result.setSource(source);
    result.setValue(value);
    return result;
  }
}
