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

import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.test.util.CommonUtils;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link GetStatusOptions}.
 */
public class GetStatusOptionsTest {
  @Test
  public void defaults() {
    GetStatusOptions options = GetStatusOptions.defaults();

    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataType.Once, options.getLoadMetadataType());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(GetStatusOptions.class);
  }

  @Test
  public void toProto() {
    GetStatusOptions options = GetStatusOptions.defaults();
    GetStatusPOptions protoOptions = GrpcUtils.toProto(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, protoOptions.getLoadMetadataType());
  }
}
