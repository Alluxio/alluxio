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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for {@link ThriftUtils}.
 */
public class ThriftUtilsTest {
  @Test
  public void toThrift() {
    WorkerNetAddress wna = getWorkerNetAddress();
    alluxio.thrift.WorkerNetAddress b = ThriftUtils.toThrift(wna);
    assertEquals(wna.getHost(), b.getHost());

    assertEquals(ThriftUtils.toThrift(TtlAction.FREE).name().toLowerCase(), TtlAction.FREE.name()
        .toLowerCase());
    
  }

  private WorkerNetAddress getWorkerNetAddress() {
    WorkerNetAddress a = new WorkerNetAddress();
    a.setHost("host1");
    a.setDataPort(1);
    a.setRpcPort(2);
    a.setWebPort(3);
    return a;
  }
}
