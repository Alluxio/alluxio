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

package alluxio.wire;

import alluxio.grpc.GrpcUtils;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MountPointInfoTest {
  @Test
  public void json() throws Exception {
    MountPointInfo mountPointInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    MountPointInfo other = mapper.readValue(mapper.writeValueAsBytes(mountPointInfo),
        MountPointInfo.class);
    checkEquality(mountPointInfo, other);
  }

  @Test
  public void proto() {
    MountPointInfo mountPointInfo = createRandom();
    MountPointInfo other = GrpcUtils.fromProto(GrpcUtils.toProto(mountPointInfo));
    checkEquality(mountPointInfo, other);
  }

  public void checkEquality(MountPointInfo a, MountPointInfo b) {
    Assert.assertEquals(a.getUfsUri(), b.getUfsUri());
    Assert.assertEquals(a.getUfsType(), b.getUfsType());
    Assert.assertEquals(a.getUfsCapacityBytes(), b.getUfsCapacityBytes());
    Assert.assertEquals(a.getUfsUsedBytes(), b.getUfsUsedBytes());
    Assert.assertEquals(a.getReadOnly(), b.getReadOnly());
    Assert.assertEquals(a.getProperties(), b.getProperties());
    Assert.assertEquals(a.getMountId(), b.getMountId());
    Assert.assertEquals(a, b);
  }

  public static MountPointInfo createRandom() {
    Random random = new Random();
    String ufsUri = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String ufsType = CommonUtils.randomAlphaNumString(random.nextInt(10));
    long ufsCapacityBytes = random.nextLong();
    long ufsUsedBytes = random.nextLong();
    long mountId = random.nextLong();
    boolean readOnly = random.nextBoolean();
    Map<String, String> properties = new HashMap<>();
    for (int i = 0, n = random.nextInt(10) + 1; i < n; i++) {
      properties.put(CommonUtils.randomAlphaNumString(random.nextInt(5)),
          CommonUtils.randomAlphaNumString(random.nextInt(5)));
    }

    MountPointInfo result = new MountPointInfo();
    result.setUfsUri(ufsUri);
    result.setUfsType(ufsType);
    result.setUfsCapacityBytes(ufsCapacityBytes);
    result.setUfsUsedBytes(ufsUsedBytes);
    result.setReadOnly(readOnly);
    result.setProperties(properties);
    result.setMountId(mountId);

    return result;
  }
}
