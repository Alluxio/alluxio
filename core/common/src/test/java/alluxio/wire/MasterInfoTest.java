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

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class MasterInfoTest {

  @Test
  public void json() throws Exception {
    MasterInfo masterInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    MasterInfo other =
        mapper.readValue(mapper.writeValueAsBytes(masterInfo), MasterInfo.class);
    checkEquality(masterInfo, other);
  }

  public void checkEquality(MasterInfo a, MasterInfo b) {
    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getAddress(), b.getAddress());
    Assert.assertEquals(a.getLastUpdatedTime(), b.getLastUpdatedTime());
    Assert.assertEquals(a.getStartTime(), b.getStartTime());
    Assert.assertEquals(a.getPrimacyChangeTime(), b.getPrimacyChangeTime());
    Assert.assertEquals(a.getVersion(), b.getVersion());
    Assert.assertEquals(a.getRevision(), b.getRevision());
    Assert.assertEquals(a, b);
  }

  public static MasterInfo createRandom() {
    Random random = new Random();
    long id = random.nextLong();
    Address address = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());
    long lastUpdatedTimeMs = CommonUtils.getCurrentMs();
    long primacyChangeTimeMs = lastUpdatedTimeMs - random.nextInt(10000);
    long startTimeMs = primacyChangeTimeMs - random.nextInt(10000);
    String version = String.format("%d.%d.%d", random.nextInt(10),
        random.nextInt(20), random.nextInt(10));
    String revision = DigestUtils.sha1Hex(RandomStringUtils.random(10));

    MasterInfo result = new MasterInfo(id, address);
    result.setLastUpdatedTime(lastUpdatedTimeMs);
    result.setStartTime(startTimeMs);
    result.setPrimacyChangeTime(primacyChangeTimeMs);
    result.setVersion(version);
    result.setRevision(revision);
    return result;
  }
}
