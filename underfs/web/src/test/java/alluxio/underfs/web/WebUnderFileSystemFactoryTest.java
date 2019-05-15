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

package alluxio.underfs.web;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link WebUnderFileSystemFactory}.
 */
public class WebUnderFileSystemFactoryTest {

  /**
   * This test ensures the web UFS module correctly accepts paths that begin with / or file://.
   */
  @Test
  public void factory() {
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    UnderFileSystemFactory factory =
        UnderFileSystemFactoryRegistry.find("https://downloads.alluxio.io/downloads/files/2.0.0-preview/", conf);
    UnderFileSystemFactory factory2 =
        UnderFileSystemFactoryRegistry.find("http://downloads.alluxio.org/", conf);
    UnderFileSystemFactory factory3 =
        UnderFileSystemFactoryRegistry.find("httpx://path", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for http paths when using this module", factory);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for http paths when using this module", factory2);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for non http paths when using this module",
        factory3);
  }
}
