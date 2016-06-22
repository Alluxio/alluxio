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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AlluxioProcess}.
 */
public final class AlluxioProcessTest {
  @Test
  public void typeTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.CLIENT);
    Assert.assertEquals(AlluxioProcess.Type.CLIENT, AlluxioProcess.getType());
    AlluxioProcess.setType(AlluxioProcess.Type.MASTER);
    Assert.assertEquals(AlluxioProcess.Type.MASTER, AlluxioProcess.getType());
    AlluxioProcess.setType(AlluxioProcess.Type.WORKER);
    Assert.assertEquals(AlluxioProcess.Type.WORKER, AlluxioProcess.getType());
  }
}
