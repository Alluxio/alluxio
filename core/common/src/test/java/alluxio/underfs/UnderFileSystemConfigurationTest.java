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
  public void getValueWhenGlobalConfHasProperty() throws Exception {
    // Set property in global configuration
    try (Closeable c = new ConfigurationRule(PropertyKey.S3A_ACCESS_KEY, "bar").toResource()) {
      UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
      Assert.assertEquals("bar", conf.getValue(PropertyKey.S3A_ACCESS_KEY));
      conf = new UnderFileSystemConfiguration(
          ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
      Assert.assertEquals("foo", conf.getValue(PropertyKey.S3A_ACCESS_KEY));
    }
  }

  @Test
  public void getValueWhenGlobalConfHasNotProperty() throws Exception {
    // Set property in global configuration
    try (Closeable c = new ConfigurationRule(PropertyKey.S3A_ACCESS_KEY, null).toResource()) {
      UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
      try {
        conf.getValue(PropertyKey.S3A_ACCESS_KEY);
        Assert.fail("this key should not exist");
      } catch (Exception e) {
        // expect to pass
      }
      conf = new UnderFileSystemConfiguration(
          ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
      Assert.assertEquals("foo", conf.getValue(PropertyKey.S3A_ACCESS_KEY));
    }
  }

  @Test
  public void containsWhenGlobalConfHasProperty() throws Exception {
    // Unset property in global configuration
    try (Closeable c = new ConfigurationRule(PropertyKey.S3A_ACCESS_KEY, "bar").toResource()) {
      UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
      Assert.assertTrue(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
      conf = new UnderFileSystemConfiguration(
          ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
      Assert.assertTrue(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
    }
  }

  @Test
  public void containsWhenGlobalConfHasNotProperty() throws Exception {
    // Unset property in global configuration
    try (Closeable c = new ConfigurationRule(PropertyKey.S3A_ACCESS_KEY, null).toResource()) {
      UnderFileSystemConfiguration conf = new UnderFileSystemConfiguration(null);
      Assert.assertFalse(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
      conf = new UnderFileSystemConfiguration(
          ImmutableMap.of(PropertyKey.S3A_ACCESS_KEY.toString(), "foo"));
      Assert.assertTrue(conf.containsKey(PropertyKey.S3A_ACCESS_KEY));
    }
  }
}
