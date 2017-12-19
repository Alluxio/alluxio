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

package alluxio.client.block.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.test.util.CommonUtils;
import alluxio.thrift.LockBlockTOptions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for the {@link LockBlockOptions} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LockBlockTOptions.class})
public class LockBlockOptionsTest {

  /**
  * Test for {@link LockBlockOptions#defaults()}.
  */
  @Test
  public void testDefaults() {
    LockBlockOptions options = LockBlockOptions.defaults();
    assertNotNull(options);
  }

  /**
  * Test for {@link LockBlockOptions#equals(Object)}.
  */
  @Test
  public void testEquals() throws Exception {
    CommonUtils.testEquals(LockBlockOptions.class);
  }

  /**
  * Test for {@link LockBlockOptions#toThrift()}.
  */
  @Test
  public void testToThrift() throws Exception {
    String ufsPath = "ufsPath";
    long blockSize = 1000L;

    alluxio.thrift.LockBlockTOptions mockObject =
        PowerMockito.mock(alluxio.thrift.LockBlockTOptions.class);
    PowerMockito.when(mockObject.getBlockSize()).thenReturn(blockSize);
    PowerMockito.when(mockObject.getUfsPath()).thenReturn(ufsPath);

    LockBlockOptions options = LockBlockOptions.defaults();
    options.setUfsPath(ufsPath);
    options.setBlockSize(blockSize);

    LockBlockTOptions thriftOptions = options.toThrift();
    assertEquals(thriftOptions.getBlockSize(), mockObject.getBlockSize());
    assertEquals(thriftOptions.getUfsPath(), mockObject.getUfsPath());
  }
}
