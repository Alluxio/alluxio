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

package alluxio.master.file.meta.options;

import alluxio.AlluxioURI;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.contexts.MountContext;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MountInfo}.
 */
public final class MountInfoTest {
  /**
   * Tests getting fields of {@link MountInfo}.
   */
  @Test
  public void getFields() {
    AlluxioURI uri = new AlluxioURI("alluxio://localhost:19998/test");
    AlluxioURI ufsUri = new AlluxioURI("hdfs://localhost:123/test2");
    MountPOptions options = MountContext.defaults().getOptions().build();
    MountInfo info = new MountInfo(uri, ufsUri, 1, options);
    Assert.assertEquals(uri, info.getAlluxioUri());
    Assert.assertEquals(ufsUri, info.getUfsUri());
    Assert.assertEquals(options, info.getOptions());
    Assert.assertEquals(1, info.getMountId());
  }
}
