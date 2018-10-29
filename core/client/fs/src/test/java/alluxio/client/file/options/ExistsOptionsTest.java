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
import alluxio.wire.LoadMetadataType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ExistsOptions} class.
 */
public class ExistsOptionsTest {
  @Test
  public void defaults() {
    ExistsOptions options = ExistsOptions.defaults();

    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataType.Once, options.getLoadMetadataType());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(ExistsOptions.class);
  }

  @Test
  public void toGetStatusOptions() {
    ExistsOptions options = ExistsOptions.defaults();
    GetStatusPOptions getStatusOptions = options.toGetStatusOptions();
    Assert.assertEquals(LoadMetadataPType.ONCE, getStatusOptions.getLoadMetadataType());
  }
}
