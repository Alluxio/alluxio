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

package alluxio.client.file.options;

import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.test.util.CommonUtils;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ListStatusOptions} class.
 */
public class ListStatusOptionsTest {
  @Test
  public void defaults() {
    ListStatusOptions options = ListStatusOptions.defaults();

    Assert.assertEquals(LoadMetadataType.Once, options.getLoadMetadataType());
    Assert.assertEquals(false, options.isRecursive());
  }

  @Test
  public void fields() {
    ListStatusOptions options = ListStatusOptions.defaults();
    Assert.assertEquals(LoadMetadataType.Once, options.getLoadMetadataType());
    Assert.assertEquals(false, options.isRecursive());
  }

  @Test
  public void toProto() {
    ListStatusOptions options = ListStatusOptions.defaults();
    ListStatusPOptions protoOptions = GrpcUtils.toProto(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, protoOptions.getLoadMetadataType());
    Assert.assertEquals(false, protoOptions.getRecursive());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(ListStatusOptions.class);
  }
}
