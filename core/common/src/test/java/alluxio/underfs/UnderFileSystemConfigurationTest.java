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

package alluxio.underfs;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public final class UnderFileSystemConfigurationTest {

  @Test
  public void getValue() throws Exception {
    try (Closeable c = new ConfigurationRule(PropertyKey.S3A_ACCESS_KEY, "bar").toResource()) {
      UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
      Assert.assertEquals("bar", conf.getValue(PropertyKey.S3A_ACCESS_KEY));
      conf = new UnderFileSystemConfiguration(
          ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
      Assert.assertEquals("foo", conf.getValue(PropertyKey.S3A_ACCESS_KEY));
    }
  }

  @Test
  public void contains() throws Exception {
    UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
    Assert.assertFalse(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
    conf = new UnderFileSystemConfiguration(
        ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
    Assert.assertTrue(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
  }
}
