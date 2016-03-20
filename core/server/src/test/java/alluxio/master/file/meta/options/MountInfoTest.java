/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta.options;

import alluxio.AlluxioURI;
import alluxio.master.file.options.MountOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MountInfo}.
 */
public class MountInfoTest {
  /**
   * Tests getting fields of {@link MountInfo}.
   */
  @Test
  public void getFieldsTest() {
    AlluxioURI uri = new AlluxioURI("alluxio://localhost:19998/test");
    MountOptions options = MountOptions.defaults();
    MountInfo info = new MountInfo(uri, options);
    Assert.assertEquals(uri, info.getUfsUri());
    Assert.assertEquals(options, info.getOptions());
  }
}
