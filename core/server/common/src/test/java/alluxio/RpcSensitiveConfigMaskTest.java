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

import alluxio.grpc.*;

import org.eclipse.jetty.server.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

public class RpcSensitiveConfigMaskTest {

  @Test
  public void maskAndToStringSimple() {
    String atest = "atest";
    Assert.assertEquals(atest,
        RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, atest)[0]);

    int i = 101;
    Assert.assertEquals("101",
        RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, i)[0]);
    String[] strings = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, i, atest);
    Assert.assertEquals("101", strings[0]);
    Assert.assertEquals("atest", strings[1]);
  }

  @Test
  public void maskAndToStringMountPOption() {
    MountPOptions.Builder mmB = MountPOptions.newBuilder();
    java.util.Map<java.lang.String, java.lang.String> prop = mmB.getMutableProperties();
    prop.put("key1", "value1");
    prop.put("aws.accessKeyId", "aws.accessKeyId");
    MountPOptions mmp = mmB.build();
    String [] masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, mmp);
    Assert.assertEquals(false, masked[0].contains("mycredential"));
    Assert.assertEquals(true, masked[0].contains("key1"));
    Assert.assertEquals(true, masked[0].contains("Masked"));
    Assert.assertEquals(true, masked[0].contains("aws.accessKeyId"));
    Assert.assertEquals(true, masked[0].contains("value1"));
  }

  @Test
  public void maskAndToStringNested() {
    UfsInfo.Builder ub = UfsInfo.newBuilder();
    ub.getPropertiesBuilder().getMutableProperties().put("key1", "value1");
    ub.getPropertiesBuilder().getMutableProperties().put("aws.accessKeyId", "aws.accessKeyId");
    UfsInfo ui = ub.build();

    String[] masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, ui);
    Assert.assertEquals(false, masked[0].contains("mycredential"));
    Assert.assertEquals(true, masked[0].contains("key1"));
    Assert.assertEquals(true, masked[0].contains("Masked"));
    Assert.assertEquals(true, masked[0].contains("aws.accessKeyId"));
    Assert.assertEquals(true, masked[0].contains("value1"));
  }

  @Test
  public void maskAndToStringRPC() {
    BlockHeartbeatPOptions.Builder hb = BlockHeartbeatPOptions.newBuilder();
    BlockHeartbeatPOptions bp = hb.build();

    String[] masked = RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskAndToString(null, bp);
  }

  @Test
  public void maskObjectsAll() {
    {
      MountPOptions.Builder mpb = MountPOptions.newBuilder();
      mpb.putProperties("key1", "value1");
      mpb.putProperties("aws.accessKeyId", "mycredential");
      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, mpb.build()));
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
      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, obj.build()));
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
      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, obj.build()));
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
      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, obj.build()));
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
      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, obj.build()));
      Assert.assertEquals(true, result.contains("key1"));
      Assert.assertEquals(true, result.contains("value1"));
      Assert.assertEquals(true, result.contains("Masked"));
      Assert.assertEquals(true, result.contains("aws.accessKeyId"));
      Assert.assertEquals(false, result.contains("mycredential"));
    }

    {
      String astr = "astr";

      String result = String.format("{%s}", RpcSensitiveConfigMask.RPCSENSITIVECMASK.maskObjects(null, astr));
      Assert.assertEquals(false, result.contains("mycredential"));
      Assert.assertEquals(false, result.contains("Masked"));
      Assert.assertEquals(true, result.contains(astr));
    }
  }
}
