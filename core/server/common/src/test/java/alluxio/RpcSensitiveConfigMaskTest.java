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

import alluxio.grpc.BlockHeartbeatPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.UfsInfo;

import org.junit.Assert;
import org.junit.Test;

public class RpcSensitiveConfigMaskTest {

  @Test
  public void maskAndToStringSimple() {
    String atest = "atest";
    Assert.assertEquals(atest,
        RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, atest));

    int i = 101;
    Assert.assertEquals("101",
        RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, i));
    Assert.assertEquals("101atest",
        RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, i, atest));
  }

  @Test
  public void maskAndToStringMountPOption() {
    MountPOptions.Builder mmB = MountPOptions.newBuilder();
    java.util.Map<java.lang.String, java.lang.String> prop = mmB.getMutableProperties();
    prop.put("key1", "value1");
    prop.put("aws.accessKeyId", "aws.accessKeyId");
    MountPOptions mmp = mmB.build();
    String masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, mmp);
    Assert.assertEquals(false, masked.contains("mycredential"));
    Assert.assertEquals(true, masked.contains("key1"));
    Assert.assertEquals(true, masked.contains("Masked"));
    Assert.assertEquals(true, masked.contains("aws.accessKeyId"));
    Assert.assertEquals(true, masked.contains("value1"));
  }

  @Test
  public void maskAndToStringNested() {
    UfsInfo.Builder ub = UfsInfo.newBuilder();
    ub.getPropertiesBuilder().getMutableProperties().put("key1", "value1");
    ub.getPropertiesBuilder().getMutableProperties().put("aws.accessKeyId", "aws.accessKeyId");
    UfsInfo ui = ub.build();

    String masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, ui);
    Assert.assertEquals(false, masked.contains("mycredential"));
    Assert.assertEquals(true, masked.contains("key1"));
    Assert.assertEquals(true, masked.contains("Masked"));
    Assert.assertEquals(true, masked.contains("aws.accessKeyId"));
    Assert.assertEquals(true, masked.contains("value1"));
  }

  @Test
  public void maskAndToStringRPC() {
    BlockHeartbeatPOptions.Builder hb = BlockHeartbeatPOptions.newBuilder();
    BlockHeartbeatPOptions bp = hb.build();

    String masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, bp);
  }
}
