/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import alluxio.Constants;
import alluxio.master.block.BlockId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with an id in Alluxio.
 */
@ThreadSafe
public final class IdUtils {
  private IdUtils() {} // prevent instantiation

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  public static final long INVALID_FILE_ID = -1;
  public static final long INVALID_WORKER_ID = -1;
  private static Random sRandom = new Random();

  /**
   * Creates an id for a file based on the given id of the container.
   *
   * @param containerId the id of the container
   * @return a file id based on the given container id
   */
  public static long createFileId(long containerId) {
    long id = BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
    if (id == INVALID_FILE_ID) {
      // Right now, there's not much we can do if the file id we're returning is -1, since the file
      // id is completely determined by the container id passed in. However, by the current
      // algorithm, -1 will be the last file id generated, so the chances somebody will get to that
      // are slim. For now we just log it.
      LOG.warn("Created file id -1, which is invalid");
    }
    return id;
  }

  /**
   * @return the created RPC id
   */
  public static String createRpcId() {
    return UUID.randomUUID().toString();
  }

  /**
   * @return a random long which is guaranteed to be non negative (zero is allowed)
   */
  public static synchronized long getRandomNonNegativeLong() {
    return Math.abs(sRandom.nextLong());
  }
}
