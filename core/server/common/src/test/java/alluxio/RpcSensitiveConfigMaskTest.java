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

import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.UfsInfo;
import alluxio.grpc.GetUfsInfoPResponse;
import alluxio.grpc.UpdateMountPRequest;

import org.junit.Assert;
import org.junit.Test;

public class RpcSensitiveConfigMaskTest {
  @Test
  public void maskObjectsAll() {
    {
      MountPOptions.Builder mpb = MountPOptions.newBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, mpb.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      MountPRequest.Builder obj = MountPRequest.newBuilder();
      MountPOptions.Builder mpb = obj.getOptionsBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, obj.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      UfsInfo.Builder obj = UfsInfo.newBuilder();
      MountPOptions.Builder mpb = obj.getPropertiesBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, obj.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      GetUfsInfoPResponse.Builder obj = GetUfsInfoPResponse.newBuilder();
      MountPOptions.Builder mpb = obj.getUfsInfoBuilder().getPropertiesBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, obj.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      UpdateMountPRequest.Builder obj = UpdateMountPRequest.newBuilder();
      MountPOptions.Builder mpb = obj.getOptionsBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, obj.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      String astr = "astr";

      String result = String.format("{%s}",
          RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(null, astr));
      Assert.assertEquals(false, result.contains("mycredential"));
      Assert.assertEquals(false, result.contains("Masked"));
      Assert.assertEquals(true, result.contains(astr));
    }
  }
}
