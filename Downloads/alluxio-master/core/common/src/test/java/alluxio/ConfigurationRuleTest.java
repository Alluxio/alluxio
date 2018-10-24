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

package alluxio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link ConfigurationRule}.
 */
public final class ConfigurationRuleTest {

  @Test
  public void changeConfiguration() throws Throwable {
    Statement statement = new Statement() {
      @Override
      public void evaluate() throws Throwable {
        assertEquals("testValue", Configuration.get(PropertyKey.MASTER_HOSTNAME));
      }
    };
    new ConfigurationRule(ImmutableMap.of(PropertyKey.MASTER_HOSTNAME, "testValue"))
        .apply(statement, null).evaluate();
  }

  @Test
  public void changeConfigurationForDefaultNullValue() throws Throwable {
    Statement statement = new Statement() {
      @Override
      public void evaluate() throws Throwable {
        assertEquals("testValue", Configuration.get(PropertyKey.SECURITY_LOGIN_USERNAME));
      }
    };
    assertFalse(Configuration.isSet(PropertyKey.SECURITY_LOGIN_USERNAME));
    new ConfigurationRule(ImmutableMap.of(PropertyKey.SECURITY_LOGIN_USERNAME, "testValue"))
        .apply(statement, null).evaluate();
    assertFalse(Configuration.isSet(PropertyKey.SECURITY_LOGIN_USERNAME));
  }
}
