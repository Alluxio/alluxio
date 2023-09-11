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

package alluxio.worker.netty;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import alluxio.proto.dataserver.Protocol;
import static org.junit.Assert.assertEquals;

import java.util.Random;

public class WriteRequestTest {
  Protocol.WriteRequest t;
  long mId;

  @Before
  public void before() {
    Random random = new Random();
    mId = random.nextLong();
    t = Protocol.WriteRequest.newBuilder()
      .setId(mId)
      .setCreateUfsFileOptions(Protocol.CreateUfsFileOptions.newBuilder().setUfsPath("test"))
      .build();
  }

  @Test
  public void testGetId() {
    assertEquals(mId, t.getId());
  }
}
