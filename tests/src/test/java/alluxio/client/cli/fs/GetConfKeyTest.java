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

package alluxio.client.cli.fs;

import static org.junit.Assert.assertEquals;

import alluxio.PropertyKey;
import alluxio.SystemOutRule;
import alluxio.cli.GetConfKey;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;

/**
 * Tests for {@link GetConfKey}.
 */
public final class GetConfKeyTest {
  private ByteArrayOutputStream mOutputStream = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mOutputStreamRule = new SystemOutRule(mOutputStream);

  @Test
  public void getConfKeyWithAllPropertyNames() throws Exception {
    for (Field field : PropertyKey.class.getDeclaredFields()) {
      if (field.getType().equals(PropertyKey.class)) {
        String key = ((PropertyKey) field.get(PropertyKey.class)).toString();
        assertConfKey(key.toUpperCase().replace(".", "_"), key, 0);
      }
    }
  }

  @Test
  public void getConfKeyWithInvalidName() throws Exception {
    assertConfKey("ALLUXIO_INVALID_COMMAND", "", 1);
  }

  private void assertConfKey(String varName, String confKey, int returnValue) {
    mOutputStream.reset();
    assertEquals(String.format("check return value of getConfKey with variable name %s", varName),
        returnValue, GetConfKey.getConfKey(varName));
    if (returnValue == 0) {
      assertEquals(String.format("check output of getConfKey with variable name %s", varName),
          confKey, mOutputStream.toString().trim());
    }
  }
}
