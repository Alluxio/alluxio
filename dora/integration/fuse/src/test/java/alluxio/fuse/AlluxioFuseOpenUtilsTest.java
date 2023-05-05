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

package alluxio.fuse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AlluxioFuseOpenUtils}.
 */
public class AlluxioFuseOpenUtilsTest {
  @Test
  public void readOnly() {
    int[] readFlags = new int[]{0x8000, 0x9000};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void writeOnly() {
    int[] readFlags = new int[]{0x8001, 0x9001};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.WRITE_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void readWrite() {
    int[] readFlags = new int[]{0x8002, 0x9002, 0xc002};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_WRITE,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }
}
