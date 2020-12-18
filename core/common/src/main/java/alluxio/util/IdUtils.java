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

package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.BlockId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with an id in Alluxio.
 */
@ThreadSafe
public final class IdUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IdUtils.class);

  public static final long INVALID_FILE_ID = -1;
  public static final long INVALID_WORKER_ID = -1;
  public static final long INVALID_MOUNT_ID = -1;
  public static final long ROOT_MOUNT_ID = 1;
  /**
   * The journal ufs is stored as a special mount in the ufs manage.
   */
  public static final long UFS_JOURNAL_MOUNT_ID = Long.MAX_VALUE - 10000;
  private static SecureRandom sRandom = new SecureRandom();

  private IdUtils() {} // prevent instantiation

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
   * Creates a file ID from a block ID.
   *
   * @param blockId the block ID
   * @return the file ID
   */
  public static long fileIdFromBlockId(long blockId) {
    return createFileId(BlockId.getContainerId(blockId));
  }

  /**
   * @return the created RPC id
   */
  public static String createRpcId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Generates a positive random number by zero-ing the sign bit.
   *
   * @return a random long which is guaranteed to be non negative (zero is allowed)
   */
  public static synchronized long getRandomNonNegativeLong() {
    return sRandom.nextLong() & Long.MAX_VALUE;
  }

  /**
   * @return a session ID
   */
  public static long createSessionId() {
    return getRandomNonNegativeLong();
  }

  /**
   * @return a random long which is guaranteed to be non negative (zero is allowed)
   */
  public static long createMountId() {
    return getRandomNonNegativeLong();
  }

  /**
   * @return app suffixed by a positive random long
   */
  public static String createFileSystemContextId() {
    return "app-" + getRandomNonNegativeLong();
  }

  /**
   *
   * @param conf an alluxio configuration with the USER_APP_ID property key
   * @return a string representing the USER_APP_ID
   */
  public static String createOrGetAppIdFromConfig(AlluxioConfiguration conf) {
    if (conf.isSet(PropertyKey.USER_APP_ID)) {
      return conf.get(PropertyKey.USER_APP_ID);
    } else {
      return IdUtils.createFileSystemContextId();
    }
  }
}
