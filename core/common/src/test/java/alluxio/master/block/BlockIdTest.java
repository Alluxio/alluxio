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

package alluxio.master.block;

import static org.junit.Assert.assertEquals;

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
  public void createBlockIdWithMaxSequenceNumber() {
    assertEquals(33554431L, BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()));
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.createBlockId(0, BlockId.getMaxSequenceNumber()));
    assertEquals(4294967295L, BlockId.createBlockId(255, BlockId.getMaxSequenceNumber()));
  }

  /**
   * Tests the {@link BlockId#createBlockId(long, long)} method.
   */
  @Test
  public void createBlockId() {
    assertEquals(16797216L, BlockId.createBlockId(1, 20000L));
    assertEquals(20000L, BlockId.createBlockId(0, 20000L));
    assertEquals(2071248101952L, BlockId.createBlockId(123456, 123456L));
  }

  /**
   * Tests the {@link BlockId#getContainerId(long)} and {@link BlockId#getSequenceNumber(long)}
   * methods.
   */
  @Test
  public void getContainerIdAndSequenceNumber() {
    assertEquals(1L, BlockId.getContainerId(33554431L));
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(33554431L));
    assertEquals(255L, BlockId.getContainerId(4294967295L));
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getSequenceNumber(4294967295L));
    assertEquals(123456L, BlockId.getContainerId(2071248101952L));
    assertEquals(123456L, BlockId.getSequenceNumber(2071248101952L));
  }

  @Test
  public void getFileId() {
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getFileId(1L));
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getFileId(20000L));
    assertEquals(MAX_SEQUENCE_NUMBER * 2 + 1, BlockId.getFileId(2 * MAX_SEQUENCE_NUMBER));
    assertEquals(2071264755711L, BlockId.getFileId(2071248101952L));
  }

  /**
   * Tests the {@link BlockId#getMaxSequenceNumber()} method.
   */
  @Test
  public void getMaxSequenceNumber() {
    assertEquals(MAX_SEQUENCE_NUMBER, BlockId.getMaxSequenceNumber());
  }
}
