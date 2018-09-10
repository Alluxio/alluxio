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

import alluxio.grpc.UfsMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.test.util.CommonUtils;
import alluxio.util.grpc.GrpcUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link UpdateUfsModeOptions} class.
 */
public class UpdateUfsModeOptionsTest {
  @Test
  public void defaults() {
    UpdateUfsModeOptions options = UpdateUfsModeOptions.defaults();

    Assert.assertNotNull(options);
    Assert.assertEquals(alluxio.underfs.UfsMode.READ_WRITE, options.getUfsMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(UpdateUfsModeOptions.class);
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    UpdateUfsModeOptions options =
        UpdateUfsModeOptions.defaults().setUfsMode(alluxio.underfs.UfsMode.READ_ONLY);
    Assert.assertEquals(alluxio.underfs.UfsMode.READ_ONLY, options.getUfsMode());
  }

  /**
   * Tests conversion to proto representation.
   */
  @Test
  public void toProto() {
    UpdateUfsModeOptions options =
        UpdateUfsModeOptions.defaults().setUfsMode(alluxio.underfs.UfsMode.NO_ACCESS);
    UpdateUfsModePOptions protoOptions = GrpcUtils.toProto(options);
    Assert.assertEquals(UfsMode.NoAccess, protoOptions.getUfsMode());
  }
}
