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

public final class BlockId {

  private static final int CONTAINER_ID_BITS = 40;
  private static final int SEQUENCE_NUMBER_BITS = 64 - CONTAINER_ID_BITS;
  private static final long CONTAINER_ID_MASK = (1L << CONTAINER_ID_BITS) - 1;
  private static final long SEQUENCE_NUMBER_MASK = (1L << SEQUENCE_NUMBER_BITS) - 1;

  private BlockId() {
    // util class
  }

  public static long createBlockId(long containerId, long sequenceNumber) {
    // TODO: check for valid ids here?
    return ((containerId & CONTAINER_ID_MASK) << SEQUENCE_NUMBER_BITS)
        | (sequenceNumber & SEQUENCE_NUMBER_MASK);
  }

  public static long getContainerId(long blockId) {
    return (blockId >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
  }

  public static long getSequenceNumber(long blockId) {
    return (blockId & SEQUENCE_NUMBER_MASK);
  }

  public static long getMaxSequenceNumber() {
    return SEQUENCE_NUMBER_MASK;
  }
}
