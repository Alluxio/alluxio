/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class LockBlockResultTest {

  @Test
  public void jsonTest() throws Exception {
    LockBlockResult lockBlockResult = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    LockBlockResult other =
        mapper.readValue(mapper.writeValueAsBytes(lockBlockResult), LockBlockResult.class);
    checkEquality(lockBlockResult, other);
  }

  @Test
  public void thriftTest() {
    LockBlockResult lockBlockResult = createRandom();
    LockBlockResult other = ThriftUtils.fromThrift(ThriftUtils.toThrift(lockBlockResult));
    checkEquality(lockBlockResult, other);
  }

  public void checkEquality(LockBlockResult a, LockBlockResult b) {
    Assert.assertEquals(a.getLockId(), b.getLockId());
    Assert.assertEquals(a.getBlockPath(), b.getBlockPath());
    Assert.assertEquals(a, b);
  }

  public static LockBlockResult createRandom() {
    LockBlockResult result = new LockBlockResult();
    Random random = new Random();

    long lockId = random.nextLong();
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String blockPath = new String(bytes);

    result.setLockId(lockId);
    result.setBlockPath(blockPath);

    return result;
  }
}
