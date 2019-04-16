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

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a set of methods related to block IDs. Each block ID is a value of long with
 * the following two parts:
 * <ul>
 * <li>The most significant 5 bytes (40 bits) represent the container ID of this block and</li>
 * <li>The least significant 3 bytes (24 bits) represent the sequence number of this block in the
 * container.</li>
 * </ul>
 */
@ThreadSafe
public final class BlockId {

  private static final int CONTAINER_ID_BITS = 40;
  private static final int SEQUENCE_NUMBER_BITS = 64 - CONTAINER_ID_BITS;
  private static final long CONTAINER_ID_MASK = (1L << CONTAINER_ID_BITS) - 1;
  private static final long SEQUENCE_NUMBER_MASK = (1L << SEQUENCE_NUMBER_BITS) - 1;

  private BlockId() {
    // prevent instantiation of a util class
  }

  /**
   * @param containerId the container ID to create the block ID with
   * @param sequenceNumber the sequence number to create the block ID with
   * @return the block ID constructed with the container ID and sequence number
   */
  public static long createBlockId(long containerId, long sequenceNumber) {
    // TODO(gene): Check for valid IDs here?
    return ((containerId & CONTAINER_ID_MASK) << SEQUENCE_NUMBER_BITS)
        | (sequenceNumber & SEQUENCE_NUMBER_MASK);
  }

  /**
   * @param blockId the block ID to get the container ID for
   * @return the container ID of a specified block ID
   */
  public static long getContainerId(long blockId) {
    return (blockId >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
  }

  /**
   * @param blockId the block ID to get the file ID for
   * @return the file ID of a specified block ID
   */
  public static long getFileId(long blockId) {
    return createBlockId(getContainerId(blockId), getMaxSequenceNumber());
  }

  /**
   * @param blockId the block ID to get the sequence number for
   * @return the sequence number of the specified block ID
   */
  public static long getSequenceNumber(long blockId) {
    return blockId & SEQUENCE_NUMBER_MASK;
  }

  /**
   * @return the maximum possible sequence number for block IDs
   */
  public static long getMaxSequenceNumber() {
    return SEQUENCE_NUMBER_MASK;
  }
}
