/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.wire;

import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

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
