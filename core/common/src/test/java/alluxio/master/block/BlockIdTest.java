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

package alluxio.master.block;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link BlockId} class.
 */
public final class BlockIdTest {

  private static final long MAX_SEQUENCE_NUMBER = 16777215L;

  /**
   * Tests that the {@link BlockId#createBlockId(long, long)} method works correctly when creating a
   * block with the maximum sequence number.
   */
  @Test
  public void createBlockIdWithMaxSequenceNumberTest() {
    Assert.assertEquals(33554431L, BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER,
        BlockId.createBlockId(0, BlockId.getMaxSequenceNumber()));
    Assert.assertEquals(4294967295L, BlockId.createBlockId(255, BlockId.getMaxSequenceNumber()));
  }

  /**
   * Tests the {@link BlockId#createBlockId(long, long)} method.
   */
  @Test
  public void createBlockIdTest() {
    Assert.assertEquals(16797216L, BlockId.createBlockId(1, 20000L));
    Assert.assertEquals(20000L, BlockId.createBlockId(0, 20000L));
    Assert.assertEquals(2071248101952L, BlockId.createBlockId(123456, 123456L));
  }

  /**
   * Tests the {@link BlockId#getContainerId(long)} and {@link BlockId#getSequenceNumber(long)}
   * methods.
   */
  @Test
  public void getContainerIdAndSequenceNumberTest() {
    Assert.assertEquals(1L, BlockId.getContainerId(33554431L));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(33554431L));
    Assert.assertEquals(255L, BlockId.getContainerId(4294967295L));
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(4294967295L));
    Assert.assertEquals(123456L, BlockId.getContainerId(2071248101952L));
    Assert.assertEquals(123456L, BlockId.getSequenceNumber(2071248101952L));
  }

  /**
   * Tests the {@link BlockId#getMaxSequenceNumber()} method.
   */
  @Test
  public void getMaxSequenceNumberTest() {
    Assert.assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getMaxSequenceNumber());
  }
}
