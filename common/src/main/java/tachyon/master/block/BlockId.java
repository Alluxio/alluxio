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

package tachyon.master.block;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a set of methods related to block Ids. Each block Id is a value of long with
 * the following two parts:
 * <ul>
 * <li>The most significant 5 bytes (40 bits) represent the container Id of this block and</li>
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
   * @param containerId the container id to create the block id with
   * @param sequenceNumber the sequence number to create the block id with
   * @return the block id constructed with the container id and sequence number
   */
  public static long createBlockId(long containerId, long sequenceNumber) {
    // TODO(gene): Check for valid ids here?
    return ((containerId & CONTAINER_ID_MASK) << SEQUENCE_NUMBER_BITS)
        | (sequenceNumber & SEQUENCE_NUMBER_MASK);
  }

  /**
   * @param blockId the block id to get the container id for
   * @return the container id of a specified block id
   */
  public static long getContainerId(long blockId) {
    return (blockId >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
  }

  /**
   * @param blockId the block id to get the sequene number for
   * @return the sequence number of the specified block id
   */
  public static long getSequenceNumber(long blockId) {
    return blockId & SEQUENCE_NUMBER_MASK;
  }

  /**
   * @return the maximum possible sequence number for block ids
   */
  public static long getMaxSequenceNumber() {
    return SEQUENCE_NUMBER_MASK;
  }
}
